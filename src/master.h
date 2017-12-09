#pragma once

#include <cstdint>
#include <chrono>
#include <queue>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc/grpc.h>
#include <grpc/support/log.h>

#include "masterworker.pb.h"
#include "masterworker.grpc.pb.h"

#include "mapreduce_spec.h"
#include "file_shard.h"
using namespace masterworker;

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		enum WorkerType {
	        	MAPPER = 1,
	        	REDUCER = 2
	    };

		// struct encompasing the states of individual requests
		struct MRWorkerCallData {
			explicit MRWorkerCallData(WorkerType type): type_(type) {}
			// Context for the client. It could be used to convey extra information to
	        // the server and/or tweak certain RPC behaviors.
	        grpc::ClientContext context_;

	        // Storage for the status of the RPC upon completion.
	        grpc::Status status_;

	        WorkerType type_;
		};

		struct MRMapperCallData : public MRWorkerCallData {
			MRMapperCallData(WorkerType type, MapRequest request)
			: MRWorkerCallData(type), request_(request) {}

			MapReply reply_;
			std::unique_ptr<grpc::ClientAsyncResponseReader<MapReply>> rpc_reader_;
			MapRequest request_;
		};

		struct MRReducerCallData : public MRWorkerCallData {
			MRReducerCallData(WorkerType type, ReduceRequest request)
			: MRWorkerCallData(type), request_(request) {}

			ReduceReply reply_;
			std::unique_ptr<grpc::ClientAsyncResponseReader<ReduceReply>> rpc_reader_;
			ReduceRequest request_;
		};


		struct MRWorkerClient {
			enum State {
				DOWN = 0,
				AVAILABLE = 1,
				BUSY = 2
			};
			MRWorkerClient(Master * master, std::shared_ptr<grpc::Channel> channel, State state)
			: master_(master), state_(state), stub_(MRWorker::NewStub(channel)) {}
			
			Master * const master_;

			std::unique_ptr<MRWorker::Stub> stub_;

			State state_;
			
			// Assembles client payload and send to server.
			void AssignMap (MapRequest map_task) {
				this->state_ = BUSY;

				MRMapperCallData* call = new MRMapperCallData(MAPPER, map_task);
				
				call->rpc_reader_ = stub_->PrepareAsyncAssignMap(&call->context_, call->request_, &master_->cq);

				call->rpc_reader_->StartCall();

				call->rpc_reader_->Finish(&call->reply_, &call->status_, (void*)call);
			}

			void AssignReduce (ReduceRequest reduce_task) {
				this->state_ = BUSY;
				
				MRReducerCallData* call = new MRReducerCallData(REDUCER, reduce_task);

				call->rpc_reader_ = stub_->PrepareAsyncAssignReduce(&call->context_, call->request_, &master_->cq);

				call->rpc_reader_->StartCall();

				call->rpc_reader_->Finish(&call->reply_, &call->status_, (void*)call);
			}

		};

		std::string worker_states[3] = {"DOWN", "AVAILABLE", "BUSY"};
		std::vector<std::string> worker_addrs; // worker ip addresses and ports
		std::vector<MapRequest> map_tasks;
		std::vector<ReduceRequest> red_tasks;
		std::vector<MRWorkerClient*> mr_workers;
		std::vector<bool> map_complete;
		std::vector<bool> red_complete;
		grpc::CompletionQueue cq;

};


bool allTrue(std::vector<bool> array) {
	for (auto elem : array) {
		if (!elem) {
			return false;
		}
	}
	return true;
}

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
	worker_addrs = mr_spec.worker_addrs;

	for (uint32_t i = 0; i < file_shards.size(); i++) {
		MapRequest request;
		request.set_shard_id(i);
		for (auto it = file_shards[i].files.begin(); it != file_shards[i].files.end(); it++) {
	        File* file = request.add_shard();
	        file->set_path(it->first);
	        file->set_start_pos(it->second.first);
	        file->set_end_pos(it->second.second);
	    }
		request.set_user_id(mr_spec.user_id);
		request.set_n_outputs(mr_spec.n_outputs);
		request.set_output_dir(mr_spec.output_dir);

		map_tasks.emplace_back(request);
		map_complete.push_back(false);
	}

	for (uint32_t i = 0; i < mr_spec.n_outputs; i++) {
		ReduceRequest request;
		request.set_part_id(i);

		request.set_user_id(mr_spec.user_id);
		request.set_n_outputs(mr_spec.n_outputs);
		request.set_output_dir(mr_spec.output_dir);
		
		red_tasks.push_back(request);
		red_complete.push_back(false);
	}
    
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	// Initialize worker clients.
	for (auto& worker_addr : worker_addrs) {
		// TODO: deallocate
		std::shared_ptr<grpc::Channel> channel = 
			grpc::CreateChannel(worker_addr, grpc::InsecureChannelCredentials());
		
		// Initial channel is always IDLE
		GPR_ASSERT(channel->GetState(true) == GRPC_CHANNEL_IDLE);
		
		// Wait for 10ms to connect 
		auto channel_state = GRPC_CHANNEL_IDLE;
		channel->WaitForStateChange(
        channel_state, std::chrono::system_clock::now() + std::chrono::milliseconds(10));
		channel_state = channel->GetState(false);

		// For ready services, set worker status as available.
		MRWorkerClient::State worker_state = MRWorkerClient::DOWN;
		if(channel_state == GRPC_CHANNEL_CONNECTING || channel_state == GRPC_CHANNEL_READY) {
			worker_state = MRWorkerClient::AVAILABLE;
		}
		std::cout << "Initial worker state is : " << worker_states[worker_state] << std::endl;
		MRWorkerClient *worker = new MRWorkerClient(this, channel, worker_state);
		mr_workers.push_back(worker);
	}




	// For every shard, send messages to workers async; Set deadline; 
	while (!allTrue(map_complete)) {
		for (int i = 0; i < mr_workers.size() && !map_tasks.empty(); i++) {
			if (!mr_workers[i]->state_ == MRWorkerClient::AVAILABLE) continue;
			mr_workers[i]->AssignMap(map_tasks.front());
			map_tasks.pop_back();
		}
		void *tag;
		bool ok = false;
		// cq.AsyncNext(&tag, &ok, std::chrono::seconds(1));
		cq.Next(&tag, &ok);


		if (ok) {
			MRMapperCallData* call = static_cast<MRMapperCallData*>(tag);
			// TODO: need to handle worker straggling, worker fail and reboot
			//  && call->error_code() == Status::DEADLINE_EXCEEDED
			// 1. push_back the map_task
			// 2. change worker state to DOWN
			if (!call->status_.ok()) {
				std::cout << "[Map phase]: RPC failed" << std::endl;
				std::cout << call->status_.error_code() << std::endl 
						  << call->status_.error_message() << std::endl
						  << call->status_.error_details() << std::endl;
				return false;
			} else {
				// Set file_locs
				MapReply *mapReply = &call->reply_;
				if (mapReply->succeed()) {
					for (int i = 0; i < mapReply->file_locs_size(); i++) {
						red_tasks[i].add_file_locs(mapReply->file_locs(i));
					}
					// TODO: clean call data 
					map_complete[call->request_.shard_id()] = true;
				} else {
					std::cout << "[Map phase]: unexpected case!" << std::endl;
					return false;
				}
			}
		}
	}

	// imd files have been set up, send reduce job 
	while (!allTrue(red_complete)) {
		for (int i = 0; i < mr_workers.size() && !red_tasks.empty(); i++) {
			if (!mr_workers[i]->state_ == MRWorkerClient::AVAILABLE) continue;
			mr_workers[i]->AssignReduce(red_tasks.back());
			red_tasks.pop_back();
		}
		void *tag;
		bool ok = false;
		cq.Next(&tag, &ok);
		// cq.AsyncNext(&tag, &ok, std::chrono::seconds(1));
		if (ok) {
			// TODO: deal with case where tag actually belongs to a Mapper call.
			MRReducerCallData* call = static_cast<MRReducerCallData*>(tag);

			if (!call->status_.ok()) {
				std::cout << "[Reduce phase]: RPC failed!" << std::endl;
				std::cout << call->status_.error_code() << std::endl 
						  << call->status_.error_message() << std::endl
						  << call->status_.error_details() << std::endl;
				return false; 
			} else if (call->reply_.succeed()) {
				 red_complete[call->request_.part_id()] = true;
			} else {
				std::cout << "[Reduce phase]: unexpected case!" << std::endl;
				return false;
			}
		}
	}
	return true;
}

	
	// 
	// For every file location, send messages to worker async
	// Wait for all responses to come back.

	/*Example for setting up deadline
	// Connection timeout in seconds
	unsigned int client_connection_timeout = 5;

	ClientContext context;

	// Set timeout for API
	std::chrono::system_clock::time_point deadline =
	    std::chrono::system_clock::now() + std::chrono::seconds(client_connection_timeout);

	context.set_deadline(deadline);
	*/
#pragma once

#include <cstdint>
#include <chrono>
#include <thread>
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

// Set timeout as global const here
const unsigned int client_rpc_timeout_in_s = 2;
const unsigned int client_connection_timeout_in_milli = 10;
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
			MRWorkerCallData(int worker_id, WorkerType type)
			: worker_id_(worker_id), type_(type) {}
			// Context for the client. It could be used to convey extra information to
	        // the server and/or tweak certain RPC behaviors.
	        grpc::ClientContext context_;
			
	        // Storage for the status of the RPC upon completion.
	        grpc::Status status_;

	        int worker_id_;
	        WorkerType type_;
		};

		struct MRMapperCallData : public MRWorkerCallData {
			MRMapperCallData(int worker_id, WorkerType type, MapRequest request)
			: MRWorkerCallData(worker_id, type), request_(request) {}
			MapReply reply_;
			std::unique_ptr<grpc::ClientAsyncResponseReader<MapReply>> rpc_reader_;
			MapRequest request_;
		};

		struct MRReducerCallData : public MRWorkerCallData {
			MRReducerCallData(int worker_id, WorkerType type, ReduceRequest request)
			: MRWorkerCallData(worker_id, type), request_(request) {}
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
			MRWorkerClient(std::string worker_addr, Master * master, std::shared_ptr<grpc::Channel> channel, State state)
			: worker_addr_(worker_addr), master_(master), state_(state), channel_(channel), stub_(MRWorker::NewStub(channel)) {}
			
			std::string worker_addr_;

			Master * const master_;

			std::unique_ptr<MRWorker::Stub> stub_;

			State state_;

			std::shared_ptr<grpc::Channel>& channel_;
			

			// Assembles client payload and send to server.
			void AssignMap (int worker_id, MapRequest map_task) {
				this->state_ = BUSY;

				MRMapperCallData* call = new MRMapperCallData(worker_id, MAPPER, map_task);
				
				std::chrono::system_clock::time_point deadline =
	    			std::chrono::system_clock::now() + std::chrono::seconds(client_rpc_timeout_in_s);

	    		call->context_.set_deadline(deadline);
				

				call->rpc_reader_ = stub_->PrepareAsyncAssignMap(&call->context_, call->request_, &master_->cq);

				call->rpc_reader_->StartCall();

				call->rpc_reader_->Finish(&call->reply_, &call->status_, (void*)call);
			}

			void AssignReduce (int worker_id, ReduceRequest reduce_task) {
				this->state_ = BUSY;
				
				MRReducerCallData* call = new MRReducerCallData(worker_id, REDUCER, reduce_task);

				std::chrono::system_clock::time_point deadline =
	    			std::chrono::system_clock::now() + std::chrono::seconds(client_rpc_timeout_in_s);

	    		call->context_.set_deadline(deadline);

				call->rpc_reader_ = stub_->PrepareAsyncAssignReduce(&call->context_, call->request_, &master_->cq);

				call->rpc_reader_->StartCall();

				call->rpc_reader_->Finish(&call->reply_, &call->status_, (void*)call);
			}

		};
		void connectWorker(grpc_connectivity_state expected_state, Master::MRWorkerClient::State expected_worker_state,
			Master::MRWorkerClient* worker);


		// std::string worker_states[3] = {"DOWN", "AVAILABLE", "BUSY"};
   		// std::string channel_states[5] = {"IDLE", "CONNECTING", "READY", "TRANSIENT_FAILURE", "SHUTDOWN"};
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
		std::cout << "map work added:";
		
		for (auto it = file_shards[i].files.begin(); it != file_shards[i].files.end(); it++) {
	        File* file = request.add_shard();
	        file->set_path(it->first);
	        file->set_start_pos(it->second.first);
	        file->set_end_pos(it->second.second);
	        std::cout << it->first << " from " << it->second.first << " to " << it->second.second << std::endl; 
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
		
		red_tasks.emplace_back(request);
		red_complete.push_back(false);
	}
    
}

void Master::connectWorker(grpc_connectivity_state expected_state, 
		MRWorkerClient::State expected_worker_state, MRWorkerClient* worker) {
	std::cout << "here 111" <<std::endl;

	while (!worker->channel_->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(client_connection_timeout_in_milli))) {}

	worker->state_ = expected_worker_state;

	std::cout << "Worker " << worker->worker_addr_ << " is up!" << std::endl;
	
	return;
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	// Initialize worker clients.
	for (auto& worker_addr : worker_addrs) {
		std::shared_ptr<grpc::Channel> channel = 
			grpc::CreateChannel(worker_addr, grpc::InsecureChannelCredentials());

		MRWorkerClient *worker = new MRWorkerClient(worker_addr, this, channel, MRWorkerClient::DOWN);
		
		std::cout << "Initially -- Worker " << worker_addr << ":" << std::endl;

		new std::thread(&Master::connectWorker, this, GRPC_CHANNEL_READY, MRWorkerClient::AVAILABLE, worker);

		mr_workers.push_back(worker);
	}
	
	// For every shard, send messages to workers async; Set deadline; 
	while (!allTrue(map_complete)) {
		bool allDown = true;
		for (MRWorkerClient* worker : mr_workers) {
			if (worker->state_ != MRWorkerClient::DOWN) allDown = false;
		}
		if (allDown) {
			std::cout << "All workers maybe down... Checking worker status before proceeding..." << std::endl;
			while (true) {
				bool up = false;
				for (MRWorkerClient* worker : mr_workers) {
					if (worker->state_ == MRWorkerClient::AVAILABLE) {
						std::cout << "Up!" << std::endl;
						up = true;
						break;
					}
				}
				if (up) {
					break;
				}
			}
		}
		for (int i = 0; i < mr_workers.size() && !map_tasks.empty(); i++) {
			// Recheck state to handle originally down workers to see if they
			// have been rebooted.
			// if (mr_workers[i]->state_ == MRWorkerClient::DOWN){
			// 	checkState(GRPC_CHANNEL_READY, MRWorkerClient::AVAILABLE, GRPC_CHANNEL_IDLE, mr_workers[i]);

			// }

			// // Recheck state of busy workers to see if they are down, if down, 
			// // set as down
			// if (mr_workers[i]->state_ == MRWorkerClient::BUSY)
			// 	checkState(GRPC_CHANNEL_IDLE, MRWorkerClient::DOWN, GRPC_CHANNEL_READY, mr_workers[i]);
			

			// originally available workers are assumed to stay that way
			// and their failure or straggling are handled via deadline mechanism.
			if (mr_workers[i]->state_ != MRWorkerClient::AVAILABLE) continue;
			std::cout << "In map phase -- remaining tasks " << map_tasks.size() << std::endl;
			
			std::cout << "Worker " << mr_workers[i]->worker_addr_ 
				<< "receiving map task" << std::endl;

			// Internally set deadline in context
			mr_workers[i]->AssignMap(i, map_tasks.back());
			mr_workers[i]->state_ == MRWorkerClient::BUSY;
			map_tasks.pop_back();

		}

		void *tag;
		bool ok = false;
		// if map_complete is not all true, that means we have pending results.
		// stragglers or failed machines will also return back a deadline_exceeded
		// msg back before deadline. So this should never block.
		cq.Next(&tag, &ok);


		if (ok) {
			MRMapperCallData* call = static_cast<MRMapperCallData*>(tag);
			// handle worker straggling, worker fail
			// 1. push_back the map_task 2. set its state as down (but it may actually be busy, no difference.)
			if (!call->status_.ok() && call->status_.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
				std::cout << "[Map phase]: Deadline exceeded" << std::endl;
				map_tasks.emplace_back(call->request_);
				mr_workers[call->worker_id_]->state_ = MRWorkerClient::DOWN;
				new std::thread(&Master::connectWorker, this, GRPC_CHANNEL_READY, MRWorkerClient::AVAILABLE, mr_workers[call->worker_id_]);
			} else if (!call->status_.ok()) {
				std::cout << "[Map phase]: RPC failed!" << std::endl;
				std::cout << call->status_.error_code() << std::endl 
						  << call->status_.error_message() << std::endl
						  << call->status_.error_details() << std::endl;
			} else {
				// set the machine state as available
				// set file_locs, ignore results that are duplicates;
				MapReply *mapReply = &call->reply_;
				mr_workers[call->worker_id_]->state_ = MRWorkerClient::AVAILABLE;
				if (mapReply->succeed() && !map_complete[call->request_.shard_id()]) {	
					for (int i = 0; i < mapReply->file_locs_size(); i++) {
						red_tasks[i].add_file_locs(mapReply->file_locs(i));
					}
					map_complete[call->request_.shard_id()] = true;
				} else {
					std::cout << "[Map phase]: unexpected case!" << std::endl;
					return false;
				}

			}

			delete call;
		}
	}
	


	// imd files have been set up, send reduce job 
	while (!allTrue(red_complete)) {
		bool allDown = true;
		for (MRWorkerClient* worker : mr_workers) {
			if (worker->state_ != MRWorkerClient::DOWN) allDown = false;
		}
		if (allDown) {
			std::cout << "All workers maybe down... Checking worker status before proceeding..." << std::endl;
			while (true) {
				bool up = false;
				for (MRWorkerClient* worker : mr_workers) {
					if (worker->state_ == MRWorkerClient::AVAILABLE) {
						std::cout << "Up!" << std::endl;
						up = true;
						break;
					}
				}
				if (up) {
					break;
				}
			}
		}
		for (int i = 0; i < mr_workers.size() && !red_tasks.empty(); i++) {
			// if (mr_workers[i]->state_ == MRWorkerClient::DOWN)
			// 	checkState(GRPC_CHANNEL_READY, MRWorkerClient::AVAILABLE, GRPC_CHANNEL_IDLE, mr_workers[i]);
			
			// if (mr_workers[i]->state_ == MRWorkerClient::BUSY)
			// 	checkState(GRPC_CHANNEL_IDLE, MRWorkerClient::DOWN, GRPC_CHANNEL_READY, mr_workers[i]);

			if (!mr_workers[i]->state_ == MRWorkerClient::AVAILABLE) continue;

			std::cout << "In reduce phase -- remaining tasks " << red_tasks.size() << std::endl;
			
			std::cout << "Worker " << mr_workers[i]->worker_addr_ 
				<< "receiving reduce task" << std::endl;


			// Internally set deadline in context
			if (!mr_workers[i]->state_ == MRWorkerClient::AVAILABLE) continue;
			mr_workers[i]->AssignReduce(i, red_tasks.back());
			mr_workers[i]->state_ == MRWorkerClient::BUSY;
			red_tasks.pop_back();
		}
		void *tag;
		bool ok = false;
		cq.Next(&tag, &ok);
		if (ok) {
			MRReducerCallData* call = static_cast<MRReducerCallData*>(tag);
			if (!call->status_.ok() && call->status_.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
				std::cout << "[Map phase]: Deadline exceeded" << std::endl;
				red_tasks.emplace_back(call->request_);
				mr_workers[call->worker_id_]->state_ = MRWorkerClient::DOWN;
				new std::thread(&Master::connectWorker, this, GRPC_CHANNEL_READY, MRWorkerClient::AVAILABLE, mr_workers[call->worker_id_]);
			} else if (!call->status_.ok()){
				std::cout << "[Reduce phase]: RPC failed!" << std::endl;
				std::cout << call->status_.error_code() << std::endl 
						  << call->status_.error_message() << std::endl
						  << call->status_.error_details() << std::endl;
				return false; 
			} else if (call->reply_.succeed()) {
				// set the machine state as available
				mr_workers[call->worker_id_]->state_ = MRWorkerClient::AVAILABLE;
				// ignore results that are duplicates or are belong to a map task
				if (call->type_ == REDUCER) 
					red_complete[call->request_.part_id()] = true;
			} else {
				std::cout << "[Reduce phase]: unexpected case!" << std::endl;
				return false;
			}

			delete call;
		}
	}

	// clean up
	// for (auto worker : mr_workers) {
	// 	delete worker;
	// }



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
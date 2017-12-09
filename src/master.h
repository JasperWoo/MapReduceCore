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
		struct MapTask {
			MapTask(uint32_t shard_id, FileShard shard, std::string user_id, uint32_t n_outputs, std::string output_dir)
			: shard_id_(shard_id), shard_(shard), user_id_(user_id), n_outputs_(n_outputs), output_dir_(output_dir) {}
			
			uint32_t shard_id_;
			FileShard shard_;
			std::string user_id_;
			uint32_t n_outputs_;
			std::string output_dir_;
		};

		struct ReduceTask {
			ReduceTask(uint32_t part_id, std::string user_id, uint32_t n_outputs, std::string output_dir)
			: part_id_(part_id), user_id_(user_id), n_outputs_(n_outputs), output_dir_(output_dir) {}
			
			uint32_t part_id_;
			std::vector<std::string> file_locs_;
			std::string user_id_;
			uint32_t n_outputs_;
			std::string output_dir_;
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
			MRMapperCallData(WorkerType type, MapTask& task)
			: MRWorkerCallData(type) {
				request_.set_shard_id(task.shard_id_);
				
				for (auto it = task.shard_.files.begin(); it != task.shard_.files.end(); it++) {
	                File* file = request_.add_shard();
	                file->set_path(it->first);
	                file->set_start_pos(it->second.first);
	                file->set_end_pos(it->second.second);
	            }

				request_.set_user_id(task.user_id_);
				request_.set_n_outputs(task.n_outputs_);
				request_.set_output_dir(task.output_dir_);
			}

			MapReply reply_;
			std::unique_ptr<grpc::ClientAsyncResponseReader<MapReply>> rpc_reader_;
			MapRequest request_;
		};

		struct MRReducerCallData : public MRWorkerCallData {
			MRReducerCallData(WorkerType type, ReduceTask& task)
			: MRWorkerCallData(type) {
				request_.set_part_id(task.part_id_);
				
				for (auto file_loc : task.file_locs_) {
					request_.add_file_locs(file_loc);
	            }

				request_.set_user_id(task.user_id_);
				request_.set_n_outputs(task.n_outputs_);
				request_.set_output_dir(task.output_dir_);
			}

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
			void AssignMap (MapTask& map_task) {
				this->state_ = BUSY;

				MRMapperCallData* call = new MRMapperCallData(MAPPER, map_task);
				
				call->rpc_reader_ = stub_->PrepareAsyncAssignMap(&call->context_, call->request_, &master_->cq);

				call->rpc_reader_->StartCall();

				call->rpc_reader_->Finish(&call->reply_, &call->status_, (void*)call);
			}

			void AssignReduce (ReduceTask& reduce_task) {
				this->state_ = BUSY;
				
				MRReducerCallData* call = new MRReducerCallData(REDUCER, reduce_task);

				call->rpc_reader_ = stub_->PrepareAsyncAssignReduce(&call->context_, call->request_, &master_->cq);

				call->rpc_reader_->StartCall();

				call->rpc_reader_->Finish(&call->reply_, &call->status_, (void*)call);
			}

		};


		int n_workers;     // # of workers
		int n_outputs; // # of output files
		std::string output_dir; // output directory
		std::string user_id; // identifier of task
		std::vector<std::string> worker_addrs; // worker ip addresses and ports
		std::queue<MapTask> map_tasks;
		std::vector<ReduceTask> red_tasks;
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
	n_workers = mr_spec.n_workers;
	n_outputs = mr_spec.n_outputs;
	output_dir = mr_spec.output_dir;
	user_id = mr_spec.user_id;
	worker_addrs = mr_spec.worker_addrs;
	for (uint32_t i = 0; i < file_shards.size(); i++) {
		MapTask map_task(i, file_shards[i], user_id, n_outputs, output_dir);
		map_tasks.push(map_task);
		map_complete.push_back(false);
	}
	for (uint32_t i = 0; i < n_outputs; i++) {
		ReduceTask red_task(i, user_id, n_outputs, output_dir);
		red_tasks.push_back(red_task);
		red_complete.push_back(false);
	}

    
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	// Initialize worker clients.
	for (auto& worker_addr : worker_addrs) {
		// TODO: deallocate
		MRWorkerClient *worker = new MRWorkerClient(this, grpc::CreateChannel(
			worker_addr, grpc::InsecureChannelCredentials()), MRWorkerClient::AVAILABLE);
		mr_workers.push_back(worker);
	}

std::cout << "here 111" <<std::endl;

	// For every shard, send messages to workers async; Set deadline; 
	while (!allTrue(map_complete)) {
		std::cout << "here III" <<std::endl;
		for (int i = 0; i < mr_workers.size() && !map_tasks.empty(); i++) {
			if (!mr_workers[i]->state_ == MRWorkerClient::AVAILABLE) continue;
			mr_workers[i]->AssignMap(map_tasks.front());
			map_tasks.pop();
			std::cout << "here XXX" <<std::endl;
		}
		void *tag;
		bool ok = false;
		// cq.AsyncNext(&tag, &ok, std::chrono::seconds(1));
		cq.Next(&tag, &ok);

std::cout << "here 222" <<std::endl;

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
						red_tasks[i].file_locs_.push_back(mapReply->file_locs(i));
					}
std::cout << "here 333" <<std::endl;
					// TODO: clean call data 
					map_complete[call->request_.shard_id()] = true;
				} else {
					std::cout << "[Map phase]: unexpected case!" << std::endl;
					return false;
				}
			}
		}
	}
std::cout << "here 444" <<std::endl;

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
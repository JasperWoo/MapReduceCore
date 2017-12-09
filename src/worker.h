#pragma once

#include <fstream>
#include <sstream>
#include <string>
#include <cstdint>
#include <mr_task_factory.h>
#include "mr_tasks.h"
#include <grpc/grpc.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"
using namespace masterworker;
using namespace std;
/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker : public MRWorker::Service {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

		grpc::Status AssignMap(grpc::ServerContext *context,
								const MapRequest *request,
								MapReply *response) override;
		grpc::Status AssignReduce(grpc::ServerContext *context,
								const ReduceRequest *request,
								ReduceReply *response) override;
	
	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		string listening_ip_addr_port;
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	listening_ip_addr_port = ip_addr_port;
}

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	// creates a new gRPC server
	grpc::ServerBuilder builder;
	builder.AddListeningPort(listening_ip_addr_port, grpc::InsecureServerCredentials()); 
	builder.RegisterService(this);
	std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
	std::cout << "server started" << std::endl;
	// Waits for the server to finish.
  	server->Wait();
	// std::cout << "worker.run()..." <<std::endl;
	// auto mapper = get_mapper_from_task_factory("cs6210");
	// mapper->map("I m just a 'dummy', a \"dummy line\"");
	// auto reducer = get_reducer_from_task_factory("cs6210");
	// reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	return true;
}

grpc::Status Worker::AssignMap(grpc::ServerContext *context,
								const MapRequest *request,
								MapReply *response) {
std::cout << "MAP 111" << std::endl;
	// grab request info
	uint32_t  n_outputs = request->n_outputs();
	uint32_t  shard_id = request->shard_id();
	const string& user_id = request->user_id();
	int shard_size = request->shard_size();
	std::string output_dir = request->output_dir();
	// acquire use map function
	std::shared_ptr<BaseMapper> user_mapper = get_mapper_from_task_factory(user_id);
std::cout << "MAP 222" << std::endl;

	// set up mapper internal.
	user_mapper->impl_ = new BaseMapperInternal();
	user_mapper->impl_->set_outputs(n_outputs, shard_id, output_dir);

	// one-by-one read the files, split into records(lines separated by '\n')
	for (int i = 0; i < shard_size; i++) {
		const File& file = request->shard(i);
		const string& path = file.path();
		streampos start_pos = file.start_pos();
		streampos end_pos = file.end_pos();
		ifstream fin(path, ios::binary);
		fin.seekg(start_pos, ios::beg);
		string record;
std::cout << "MAP 333" << std::endl;

		// passing to map 
		while (getline(fin, record)) {
			std::cout << "MAP III" << std::endl;
			user_mapper->map(record);
		}

		fin.close();
	}
std::cout << "MAP 444" << std::endl;

	// write response
	response->set_succeed(true);
	for (int i = 0; i < user_mapper->impl_->output_files.size(); i++) {
		response->add_file_locs(user_mapper->impl_->output_files[i]);
	}

	// always return ok 
	return grpc::Status();
}


grpc::Status Worker::AssignReduce(grpc::ServerContext* context, const ReduceRequest* request,
		 ReduceReply* reply){
		std::string user_id = request->user_id();
		std::shared_ptr<BaseReducer> user_reducer = get_reducer_from_task_factory(user_id);
		//read all the inputFiles and arrange them into an unordered map;
		std::string line;
		std::map<std::string , std::vector<std::string>> resource;
		for(const std::string& str : request->file_locs()){
			std::ifstream intermediate_file(str);
			if(intermediate_file.is_open()){
				while(getline(intermediate_file,line)){
					//information should be saved and accumlate until send to reducer for processing
					std::istringstream linestream(line);
					std::string key,value;
					linestream>>key;
					linestream>>value;
					resource[key].push_back(value);
				}
			}
			intermediate_file.close();			
		}
		
		//once done, we can invoke reducer to do the work.
		//but first, provide the config to imp_
		user_reducer->impl_->set_n_output(request->n_outputs());
		user_reducer->impl_->set_output_folder(request->output_dir());

		for(auto itr = resource.begin();itr != resource.end();itr++){
			user_reducer->reduce(itr->first,itr->second);
		}
		//these should be dumped to the same file.
		//and write-to-file should be done by emit function in baseReducerInternal.
		reply->set_succeed(true);
		return grpc::Status::OK;


	}

		
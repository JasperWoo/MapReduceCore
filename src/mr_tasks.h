#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <cstdint>
#include <fstream>

#include <unistd.h>
#define PATH_MAX 200

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/		
		std::vector<std::string> output_files;
		std::vector<std::ofstream> ofs;
		void set_outputs(std::uint32_t n_outputs, std::uint32_t shard_id, std::string output_dir);
		void close_output(); 
};

inline void BaseMapperInternal::close_output(){
	for(auto it = ofs.begin();it!=ofs.end();it++){
		it->close();
	}
}

/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}


/* CS6210_TASK Implement this function */
// collect output from user map function, write (periodically maybe) to intermediate files.
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	std::size_t h = std::hash<std::string>{}(key);
	std::size_t idx = h % output_files.size();
	ofs[idx] << key << " " << val << "\n";
}
 
inline void BaseMapperInternal::set_outputs(std::uint32_t n_outputs, std::uint32_t shard_id, std::string output_dir) {
	
	for (int i = 0; i < n_outputs; i++) {
		output_files.push_back(output_dir + "/med_part_" + std::to_string(i) + "_num_" + std::to_string(shard_id));
		
		std::cout << output_files[i]<<std::endl;
		
		ofs.emplace_back(std::ofstream(output_files[i], std::ios::binary | std::ios::app));
	}
}
/*-----------------------------------------------------------------------------------------------*/

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
	BaseReducerInternal();

		/* DON'T change this function's signature */
	void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
	void set_output_folder(const std::string& path);
	void set_n_output(const int i);

private:
	int n_output;
	std::string output_folder;
	std::hash<std::string> str_hash;
};

inline void BaseReducerInternal::set_output_folder(const std::string& path){
	output_folder = path;
}

inline void BaseReducerInternal::set_n_output(const int i){
	n_output = i;
}

/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {
}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	unsigned key_hash = str_hash(key) % n_output;
	std::string file_path = output_folder + "/result_part_" + std::to_string(key_hash);
	std::ofstream fout;
	fout.open(file_path,std::ofstream::app);
	if(fout.is_open()==false){
		std::cout << "cannot open output file "<<file_path;
		exit(0);
	}
	std::string entry = key+" "+val+"\n";
	fout.write(entry.c_str(),entry.size());
	fout.close();

}
#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <cstdint>
#include <fstream>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/		
		std::vector<std::string> output_files;
		std::vector<std::ofstream *> ofs;
		void set_outputs(std::uint32_t n_outputs, std::uint32_t shard_id);
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}


/* CS6210_TASK Implement this function */
// collect output from user map function, write (periodically maybe) to intermediate files.
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	std::size_t h = std::hash<std::string>{}(key);
	std::size_t idx = h % output_files.size();
	*(ofs[idx]) << key << " " << val << "\n";
}
 
inline void BaseMapperInternal::set_outputs(std::uint32_t n_outputs, std::uint32_t shard_id) {
	for (int i = 0; i < n_outputs; i++) {
		output_files.push_back("output/med_" + std::to_string(shard_id) + std::to_string(i) + ".txt");
		std::ofstream fout(output_files[i], std::ios::binary | std::ios::app);
		ofs.push_back(&fout);
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
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
}

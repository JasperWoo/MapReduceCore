#pragma once

#include <vector>
#include <unordered_map>
#include <fstream>
#include <climits>
#include <math.h>
#include "mapreduce_spec.h"

using namespace std;

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
	// string being the file path, pair contains begin position and end position of the file portion.
	unordered_map<string, pair<streampos, streampos>> files;
};

streamsize get_file_size(const string& filename) {
    ifstream fin(filename, ios::ate | ios::binary);
    streamsize size =  fin.tellg(); 
    fin.close();
    return size;
}


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	int map_size = mr_spec.map_size;
	vector<string> filepaths = mr_spec.input_files;
	streamsize totalsize = 0;
	for (auto& filepath : filepaths) {
		totalsize += get_file_size(filepath);
	}
	int n_shards = (int)ceil(totalsize / 1024.0 / map_size);
	fileShards.reserve(n_shards);

	// Actual sharding. For each input file, sha
	for (auto& filepath : filepaths) {
		streamsize filesize = get_file_size(filepath);
		cout << "input file size:" << filesize / 1024.0 << endl;
		streamsize remain = filesize;
		// streampos offset = 0;
		ifstream fin(filepath, ios::binary);
		while (remain > 0) {
			streamsize extract_size = map_size;
			FileShard cur; 
			if (!fileShards.empty()) {
				FileShard last_shard = fileShards.back();
				totalsize = 0;
				cout <<endl<<endl<< "Entered key point 111" <<endl<<endl<<endl;
				for (auto it = last_shard.files.begin(); it != last_shard.files.end(); ++it) {
					totalsize += it->second.second - it->second.first;
				}
				cout <<endl<<endl<< "Totalsize:" << totalsize<< endl<<endl<<endl;
				
				if (totalsize / 1024.0 < map_size) {
					// the last shard is not yet finished
					extract_size = map_size - floor(totalsize / 1024.0);
					cout <<endl<<endl<< "Extract_size updated to " << extract_size <<endl<<endl<<endl;
					fileShards.pop_back();
					cur = last_shard;
					cout <<endl<<endl<< "Entered key point 222" <<endl<<endl<<endl;
				}
			}
			streampos start = fin.tellg();
			cout << "tell_get of start : " << start / 1024.0 << endl;
			// in c++11, seek is able to seek beyond the eof.
			fin.seekg(extract_size * 1024, ios::cur);
			streampos end = fin.tellg();
			cout << "tell_get of end : " << end / 1024.0 << endl;
			
			if (end > filesize) {
				fin.seekg(0, ios::end);
				end = fin.tellg();
				remain -= (end - start + 1);
			} else {
				fin.ignore(LONG_MAX, '\n');
				// seek back to last character in the prev line.
				fin.seekg(-2, ios::cur);
				end = fin.tellg();
				// include the newline character.
				remain -= (end - start + 2);
				// seek to new line
				fin.seekg(2, ios::cur);
			}
			cout << "tell_get of end after adjustment : " << end / 1024.0 << endl;
			cur.files[filepath] = make_pair(start, end);
			fileShards.push_back(cur);
		}
		fin.close();
		cout << endl<<endl<<endl;
	}
	return true;
}

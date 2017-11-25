#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <utility>
using namespace std;
/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	int n_workers;     // # of workers
	int n_outputs; // # of output files
	int map_size; // size (in kB) of map task input	
	string output_dir; // output directory
	string user_id; // identifier of task
	vector<string> worker_addrs; // worker ip addresses and ports
	vector<string> input_files;
};


/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const string& config_filename, MapReduceSpec& mr_spec) {
	unordered_map<string, vector<string>> values;
	ifstream fin(config_filename);
	if (!fin.fail()) {
		try {
			string line;
			while (getline(fin, line)) {
				istringstream is_line(line);
				string key;
				if (getline(is_line, key, '=')) {
					string value;
					if (!getline(is_line, value)) break;
					vector<string> records;
					istringstream is_value(value);
					while (is_value) {
						string record;
						if (!getline(is_value, record, ',')) break;
						records.push_back(record);
					}
					values[key] = records;
				}
			}
		} catch (exception& e) {
			cerr << "[ERROR] : " << e.what() << "Failed parsing config file." << std::endl;
		}
		
		fin.close();
		try {
			mr_spec.n_workers = stoi(values["n_workers"][0]);
			mr_spec.n_outputs = stoi(values["n_output_files"][0]);
			mr_spec.map_size = stoi(values["map_kilobytes"][0]);
		} catch(exception& e) {
			cerr << "[ERROR] in Spec: " << e.what() << " failed to parse integer" << endl;
			return false;
		}
		mr_spec.output_dir = values["output_dir"][0];
		mr_spec.user_id = values["user_id"][0];
		// TODO: use move() for the following two.
		mr_spec.worker_addrs = values["worker_ipaddr_ports"];
		mr_spec.input_files = values["input_files"];
		return true;
	} else {
		return false;
	}
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	// validate appropriate number of ip_addr_ports.
	if (mr_spec.n_workers <= 0 || mr_spec.n_workers != mr_spec.worker_addrs.size()) {
		cerr << "[ERROR] in Spec: invalid number of workers." << endl;
		return false;
	}
	if (mr_spec.n_outputs <= 0) {
		cerr << "[ERROR] in Spec: invalid number of output files." << endl;
		return false;
	}
	if (mr_spec.map_size <= 0) {
		cerr << "[ERROR] in Spec: invalid map size." << endl;
		return false;
	}
	if (mr_spec.output_dir.c_str() == nullptr) {
		cerr << "[ERROR] in Spec: output directory undefined." << endl;
		return false;
	}
	if (mr_spec.user_id.c_str() == nullptr) {
		cerr << "[ERROR] in Spec: user id undefined." << endl;
		return false;
	}
	// validate input file path
	for (auto& filepath : mr_spec.input_files) {
		ifstream fin(filepath);
		if (fin.fail()) {
			cerr << "[ERROR] in Spec: file " << filepath << " does not exist!" << endl;
			return false;
		} 
	}
	// validate worker address ports
	for (auto& worker_addr : mr_spec.worker_addrs) {
		istringstream is_addr(worker_addr);
		string hostname;
		if (getline(is_addr, hostname, ':')) {
			string port;
			if (hostname.compare("localhost") == 0 && getline(is_addr, port)) {
				int port_number;
				try {
					port_number = stoi(port);
				} catch (exception& e) {
					cerr << "[ERROR] in Spec: " << e.what() << " failed to parse port number" << endl;
					return false; 
				}
				if (port_number < 0 || port_number > 65535) {
					cerr << "[ERROR] in Spec: invalid port number" << endl;
					return false;
				} 
			} else {
				cerr << "[ERROR] in Spec: invalid host name" << endl;
				return false;
			}
		} else {
			cerr << "[ERROR] in Spec: invalid worker address" << endl;
			return false;
		}
	}

	return true; // all good!
}

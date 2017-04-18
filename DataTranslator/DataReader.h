#pragma once
#include <unordered_map>
#include <fstream>
#include <string>
#include "DataAgent.h"

using namespace std;

// data provider - reads file from disk and puts lines to processing queue
class DataReader: public DataAgent
{
private:
	ifstream m_inputFile;
	string m_inputFileName;

public:
	DataReader(string const & inputFile,sync_bounded_queue<Line>&);
	~DataReader();

	void Run() override;
	
};


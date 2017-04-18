#pragma once
#include <boost/thread/sync_bounded_queue.hpp>
#include <fstream>
#include "DataAgent.h"

using namespace boost;

//data consumer - writes file to disk
class DataWriter: public DataAgent
{
private:
	ofstream m_outputFile;
public:
	DataWriter(string const & outputFile, sync_bounded_queue<Line>&);
	~DataWriter();

	void Run();
};


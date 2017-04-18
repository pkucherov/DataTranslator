#include "stdafx.h"
#include "DataReader.h"
#include <fstream>

using namespace std;

DataReader::DataReader(string const & inputFile, sync_bounded_queue<Line>& queue) :DataAgent(queue), m_inputFile(inputFile), m_inputFileName(inputFile)
{
}


DataReader::~DataReader()
{
}

void DataReader::Run()
{
	int i = 0;
	if (m_inputFile.is_open())
	{
		string line;
		while (getline(m_inputFile, line))
		{			
			m_processingQueue.push_back(Line(i, line));
			i++;
		}
		m_processingQueue.close();
	}
	else
	{
		//log error
		cout << "Cannot open file " << m_inputFileName << endl;
	}
	m_processedRows = i;
}

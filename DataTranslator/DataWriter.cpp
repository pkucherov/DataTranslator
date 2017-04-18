#include "stdafx.h"
#include "DataWriter.h"
#include <fstream>


DataWriter::DataWriter(string const & outputFile, sync_bounded_queue<Line>& queue):DataAgent(queue), m_outputFile(outputFile)
{
}


DataWriter::~DataWriter()
{
}


void DataWriter::Run()
{
	int processedRows = 0;
	if (m_outputFile.is_open())
	{
		Line outLine;
		try
		{			
			while (m_processingQueue.wait_pull_front(outLine) != queue_op_status::closed)
			{
				m_outputFile << outLine.Content << endl;
				processedRows++;
			}			
		}
		catch (sync_queue_is_closed const& ex)
		{
		}
	}
	else
	{

	}

	m_processedRows = processedRows;
}
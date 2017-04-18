#pragma once
#include <boost/thread/sync_bounded_queue.hpp>
#include <string>

using namespace std;
using namespace boost;

// line from file with line number
struct Line
{
	int Number;
	string Content;

	Line() {}
	Line(int n, string const& str) : Number(n), Content(str) {}
};

// base class for data provider and consumer (reader and writer)
class DataAgent
{
protected:
	sync_bounded_queue<Line>& m_processingQueue;
	int m_processedRows;

	DataAgent(sync_bounded_queue<Line>&);
public:
	virtual void Run() = 0;
	virtual ~DataAgent();

	int GetProcessedRows() const { return m_processedRows; }

};
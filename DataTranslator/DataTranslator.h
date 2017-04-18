#pragma once
#include <string>
#include <unordered_map>
#include <boost/thread/sync_queue.hpp>
#include <boost/thread/sync_bounded_queue.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/thread.hpp>
#include "DataReader.h"
#include "ProcessController.h"

using namespace std;
using namespace boost;


// controller for translation threads
class DataTranslator
{
private:
	const int m_inputQueueSize = 20000;
	const int m_outputQueueSize = 20000;
	const int m_maxThread = 4;  //number of threads, which do the translation
	string m_strDataFolder;
	sync_queue<InOutFile>& m_fileQueue;
	sync_queue<ProcessingStatistics>& m_statQueue;
	unordered_map<string, string>& m_mapColumns;
	unordered_map<string, string>& m_mapIdentifier;

public:
	DataTranslator(unordered_map<string, string>&, unordered_map<string, string>&, sync_queue<InOutFile>&, sync_queue<ProcessingStatistics>&);
	~DataTranslator();

	void Run();
};

struct VendorData
{
	string Identifier;
	unordered_map<string, string> Values; // values in map where key is column name and value is data value
};

// thread where translation occurse
// data consumer for data reader and data provider for data writer
class TranslationThread;
class TranslationThread
{
private:
	sync_bounded_queue<Line>& m_inputQueue; // input line queue 
	sync_bounded_queue<Line>& m_outputQueue; // output line queue
	unordered_map<string, string>& m_mapColumns;
	unordered_map<string, string>& m_mapIdentifier;
	vector<string>& m_aSourceColumns;
	vector<string>& m_aTargetColumns;

	condition_variable& m_condColumnsInit;
	boost::mutex& m_mutColumnsInit;
	bool& m_bColumnsInit;

	boost::condition_variable& m_condOutQueueClose;
	boost::mutex& m_mutOutQueueClose;
	map<TranslationThread*, bool>& m_mapOutQueueClose;

	boost::mutex& m_mutThreads;
	vector<thread*>& m_aThreads;  // vector of threads for thread interrupting

	bool & m_bProcessingError;

	bool MakeOutputLine(Line const& in, string& out);
	bool DecodeLine(string const& in, VendorData& out);
	string EncodeLine(VendorData const& in);
	bool MakeLabels(string const& in, string& out);
	bool TranslateLine(VendorData const& in, VendorData& out);

public:
	TranslationThread(unordered_map<string, string>&, unordered_map<string, string>&, 
		sync_bounded_queue<Line>& input, sync_bounded_queue<Line>& output, vector<string>&, vector<string>&, 
		condition_variable&, boost::mutex&, bool&, condition_variable&, boost::mutex&, 
		map<TranslationThread*, bool>&, boost::mutex&, vector<thread*>&, bool&);
	~TranslationThread();

	void Run();  //data translation

};




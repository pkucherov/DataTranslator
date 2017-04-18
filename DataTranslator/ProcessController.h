#pragma once
#include <string>
#include <set>
#include <unordered_map>
#include <boost/thread/sync_queue.hpp>

using namespace boost;
using namespace std;

// file processing statistics
struct ProcessingStatistics
{
	int InputRows;
	int OutputRows;
	bool Error;  //true - if error occurred during file processing

	ProcessingStatistics(): Error(false), InputRows(0), OutputRows(0) {}
	ProcessingStatistics(int inRows, int outRows, bool bError) :InputRows(inRows), OutputRows(outRows), Error(bError) {}
};

// contains input file name and output file name
struct InOutFile
{
	string Input;
	string Output;

	InOutFile() {}
	InOutFile(string const& in, string const& out) : Input(in), Output(out) {}
};

//controller, which runs threads for file translation
class ProcessController
{
private:
	const int m_maxProcessorThreads = 3;  // number of threads for file processing
	const string m_columnMappingFileName = "column_mapping.tsv";
	const string m_identifierMappingFileName = "identifier_mapping.tsv";
	const string m_dataFileName = "dataFile";
	const string m_dataOutFileName = "outputFile";
	const string m_dataFileExt = "tsv";
	string m_strDataFolder;			// path to the folder where configuration,
									// data and output files are located

	unordered_map<string, string> m_mapColumns;
	unordered_map<string, string> m_mapIdentifier;

	sync_queue<InOutFile> m_queueDataFiles; // queue of input/output files
	sync_queue<ProcessingStatistics> m_queueFileProcessingStatistics;// queue of statistics

private:
	void ReadDataFolder();
	void ReadColumnMapping();
	void ReadIdentifierMapping();
	void ReadMapFromFile(unordered_map<string, string> & map, string const & filePath);
	void RunThreads();
	// creates output file names and fills file names queue
	void MakeInOutDataFileQueue(set<string>&);
	void WriteStatistics();
	void InitLogging();

public:
	ProcessController(string strFolder);
	~ProcessController();

	void Run();
};


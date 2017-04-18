#include "stdafx.h"
#include "ProcessController.h"
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <iostream>
#include <fstream>
#include <boost/tokenizer.hpp>
#include <boost/thread/thread.hpp>
#include <boost/regex.hpp>
#include <set>
#include <boost/format.hpp>

#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/global_logger_storage.hpp>

#include "DataTranslator.h"


namespace logging = boost::log;
namespace src = boost::log::sources;

namespace fs = boost::filesystem;
using namespace boost;
using namespace std;
using namespace logging::trivial;

BOOST_LOG_INLINE_GLOBAL_LOGGER_DEFAULT(app_logger, src::severity_logger_mt< severity_level >)

ProcessController::ProcessController(string strFolder) : m_strDataFolder(strFolder)
{
}


ProcessController::~ProcessController()
{
}

void ProcessController::InitLogging()
{
	logging::add_file_log("DataTranslatorLog.log");

	logging::core::get()->set_filter
	(
		logging::trivial::severity >= logging::trivial::info
	);
	logging::add_common_attributes();

	using namespace logging::trivial;

	src::severity_logger_mt< severity_level >& lg = app_logger::get();
	BOOST_LOG_SEV(lg, logging::trivial::info) << "Started";
}

void ProcessController::Run()
{
	try
	{
		InitLogging();

		//Read folder content
		ReadDataFolder();
		ReadColumnMapping();
		ReadIdentifierMapping();
		//Run data translation in several threads
		RunThreads();
		WriteStatistics();
	}
	catch (runtime_error const& ex)
	{
		src::severity_logger_mt< severity_level >& lg = app_logger::get();
		string err = string("Error during execution: ") + string(ex.what());
		BOOST_LOG_SEV(lg, logging::trivial::error) << err;

		cout << err << endl;
	}
}

void ProcessController::WriteStatistics()
{
	int readedRows = 0;
	int writedRows = 0;
	int nProcessedFiles = 0;
	int nErrorFiles = 0;

	while (!m_queueFileProcessingStatistics.empty())
	{
		ProcessingStatistics stat = m_queueFileProcessingStatistics.pull();
		readedRows += stat.InputRows;
		writedRows += stat.OutputRows;
		if (stat.Error)
		{
			nErrorFiles++;
		}
		nProcessedFiles++;
	}

	cout << "Total files processed = " << nProcessedFiles << endl;
	cout << "Total files with error = " << nErrorFiles << endl;
	cout << "Total available rows = " << readedRows << endl;
	cout << "Total processed rows = " << writedRows << endl;
}

void ProcessController::ReadDataFolder()
{
	fs::path full_path(fs::initial_path<fs::path>());
	full_path = fs::system_complete(fs::path(m_strDataFolder));
	regex dataFileFilter("dataFile.*\\.tsv");
	set<string> sortedInputFleList;

	if (!fs::exists(full_path))
	{
		std::cout << "\nNot found: " << full_path.filename() << std::endl;
		return;
	}

	if (fs::is_directory(full_path))
	{
		fs::directory_iterator end_iter;
		fs::directory_iterator dir_itr(full_path);

		for (; dir_itr != end_iter; ++dir_itr)
		{
			try
			{
				if (fs::is_regular_file(dir_itr->status()))
				{
					const string strFile = dir_itr->path().filename().string();
					if (regex_match(strFile, dataFileFilter))
					{
						sortedInputFleList.insert(strFile);
					}
				}
			}
			catch (const std::exception & ex)
			{
				std::cout << dir_itr->path().filename() << " " << ex.what() << std::endl;
			}
		}
	}

	MakeInOutDataFileQueue(sortedInputFleList);
}

// creates output file names and fills file names queue
void ProcessController::MakeInOutDataFileQueue(set<string>& files)
{
	int i = 0;
	while (files.size() > 0)
	{
		string strFile = m_dataFileName + str(format("%1%") % i) + "." + m_dataFileExt;
		if (files.find(strFile) != files.cend())
		{
			string strOutFile = m_dataOutFileName + str(format("%1%") % i) + "." + m_dataFileExt;
			m_queueDataFiles.nonblocking_push(InOutFile(m_strDataFolder + "/" + strFile, m_strDataFolder + "/" + strOutFile));
			files.erase(strFile);
		}
		else
		{
			src::severity_logger_mt< severity_level >& lg = app_logger::get();
			string err = string("Data file: ") + strFile + " missed.";
			BOOST_LOG_SEV(lg, logging::trivial::warning) << err;

			cout << err << endl;
		}
		i++;
	}
}

void ProcessController::ReadMapFromFile(unordered_map<string, string> & map, string const & filePath)
{
	string mappingFile(filePath);

	ifstream in(mappingFile.c_str());
	if (!in.is_open())
	{
		string message = string("Cannot open mapping file:") + mappingFile;
		throw runtime_error(message);
	}		

	typedef tokenizer<char_separator<char> >  Tokenizer;
	char_separator<char> sep("\t");
	string line;

	while (getline(in, line))
	{
		Tokenizer tok(line, sep);
		vector< string > vec;
		vec.assign(tok.begin(), tok.end());
		map.insert({ vec[0], vec[1] });
	}
}

void ProcessController::ReadColumnMapping()
{
	string data(m_strDataFolder + "/" + m_columnMappingFileName);

	ReadMapFromFile(m_mapColumns, data);
}

void ProcessController::ReadIdentifierMapping()
{
	string data(m_strDataFolder + "/" + m_identifierMappingFileName);

	ReadMapFromFile(m_mapIdentifier, data);
}

void ProcessController::RunThreads()
{
	boost::thread_group threadsTransaltion;

	vector<std::shared_ptr<DataTranslator>> aTranslators;

	for (int i = 0; i < m_maxProcessorThreads; i++)
	{
		std::shared_ptr<DataTranslator> translator(new DataTranslator(m_mapColumns, m_mapIdentifier, m_queueDataFiles, m_queueFileProcessingStatistics));
		aTranslators.push_back(translator);
	}

	for (auto translator : aTranslators)
	{
		if (translator != nullptr)
		{
			threadsTransaltion.create_thread(boost::bind(&DataTranslator::Run, translator));
		}
	}

	threadsTransaltion.join_all();
}
#include "stdafx.h"
#include "DataTranslator.h"
#include "DataReader.h"
#include "DataWriter.h"
#include <memory>
#include <boost/thread/thread.hpp>
#include <boost/thread/sync_bounded_queue.hpp>
#include <boost/tokenizer.hpp>
#include <boost/format.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <algorithm>

using namespace std;
using namespace boost;
namespace logging = boost::log;
using namespace logging::trivial;
namespace src = boost::log::sources;

BOOST_LOG_INLINE_GLOBAL_LOGGER_DEFAULT(app_logger, src::severity_logger_mt< severity_level >)


DataTranslator::DataTranslator(unordered_map<string, string>& mapColumns, unordered_map<string, string>& mapIdentifier, sync_queue<InOutFile>& fileQueue, sync_queue<ProcessingStatistics>& statQueue):
	m_fileQueue(fileQueue), m_statQueue(statQueue), m_mapColumns(mapColumns), m_mapIdentifier(mapIdentifier)
{
}

DataTranslator::~DataTranslator()
{
}

void DataTranslator::Run()
{	
	InOutFile inOutFile;
	while (m_fileQueue.try_pull(inOutFile) == queue_op_status::success)
	{
		cout << "Processing file " << inOutFile.Input << endl;

		vector<string> aSourceColumns;
		vector<string> aTargetColumns;
		condition_variable condColumnsInit;
		boost::mutex mutColumnsInit;
		bool bColumnsInit = false;

		boost::condition_variable condOutQueueClose;
		boost::mutex mutOutQueueClose;
		map<TranslationThread*,bool> mapOutQueueClose;

		boost::mutex mutThreads;
		vector<thread*> aThreads;

		bool fileProcessingError = false;
		{
			boost::thread_group threadsTranslation;
			sync_bounded_queue<Line> inputQueue(m_inputQueueSize);
			sync_bounded_queue<Line> outputQueue(m_outputQueueSize);

			unique_ptr<DataReader> pReader(new DataReader(inOutFile.Input, inputQueue));
			std::shared_ptr<DataWriter> pWriter(new DataWriter(inOutFile.Output, outputQueue));

			threadsTranslation.create_thread(boost::bind(&DataWriter::Run, pWriter));

			vector<std::shared_ptr<TranslationThread>> aTranslationThreads;

			for (int i = 0; i < m_maxThread; i++)
			{
				std::shared_ptr<TranslationThread> thread(new TranslationThread(m_mapColumns, m_mapIdentifier, inputQueue, outputQueue,
					aSourceColumns, aTargetColumns, condColumnsInit, mutColumnsInit, bColumnsInit, condOutQueueClose, mutOutQueueClose, 
					mapOutQueueClose, mutThreads, aThreads, fileProcessingError));
				aTranslationThreads.push_back(thread);
				mapOutQueueClose[thread.get()] = true;
			}

			for (int i = 0; i < m_maxThread; i++)
			{
				boost::thread* pThread = threadsTranslation.create_thread(boost::bind(&TranslationThread::Run, aTranslationThreads[i]));
				{
					boost::lock_guard<boost::mutex> lock(mutThreads);
					aThreads.push_back(pThread);
				}
			}

			pReader->Run();

			threadsTranslation.join_all();

			int inRows = pReader->GetProcessedRows(), outRows = pWriter->GetProcessedRows();
			m_statQueue.push(ProcessingStatistics(inRows, outRows, fileProcessingError));

			src::severity_logger_mt< severity_level >& lg = app_logger::get();
			string msg = string("Data File: ") + inOutFile.Input + " has been processed. ";
			BOOST_LOG_SEV(lg, logging::trivial::info) << msg;

			cout << msg << endl;
		}
	}
}

///////////////////////////////////////////////////////////////////////////////////////
//

TranslationThread::TranslationThread(unordered_map<string, string>& mapColumns, unordered_map<string, string>& mapIdentifier, 
	sync_bounded_queue<Line>& input, sync_bounded_queue<Line>& output, vector<string>& aSourceColumns, vector<string>& aTargetColumns, 
	condition_variable& condColumnsInit, boost::mutex& mutColumnsInit, bool& bColumnsInit,
	condition_variable& condOutQueueClose, boost::mutex& mutOutQueueClose, map<TranslationThread*, bool>& mapOutQueueClose,
	boost::mutex& mutThreads, vector<thread*>& aThreads, bool& bError):
	m_inputQueue(input), m_outputQueue(output), m_mapColumns(mapColumns), m_mapIdentifier(mapIdentifier), m_aSourceColumns(aSourceColumns),
	m_aTargetColumns(aTargetColumns), m_condColumnsInit(condColumnsInit), m_mutColumnsInit(mutColumnsInit), m_bColumnsInit(bColumnsInit),
	m_condOutQueueClose(condOutQueueClose), m_mutOutQueueClose(mutOutQueueClose), m_mapOutQueueClose(mapOutQueueClose), m_mutThreads(mutThreads),
	m_aThreads(aThreads), m_bProcessingError(bError)
{
}

TranslationThread::~TranslationThread() 
{
}


void TranslationThread::Run()
{
	Line inputLine;
	try
	{				
		for(;;)		
		{
			{
				boost::lock_guard<boost::mutex> lock(m_mutOutQueueClose);
				m_mapOutQueueClose[this] = false;				
			}
			m_condOutQueueClose.notify_all();
			queue_op_status inStatus = m_inputQueue.wait_pull_front(inputLine);
			if (inStatus == queue_op_status::closed)
			{
				{
					boost::lock_guard<boost::mutex> lock(m_mutOutQueueClose);
					m_mapOutQueueClose[this] = true;
				}
				m_condOutQueueClose.notify_all();
				break;
			}
			boost::this_thread::interruption_point();

			string outputLine;
			if (MakeOutputLine(inputLine, outputLine))
			{
				m_outputQueue.push_back(Line(inputLine.Number, outputLine));				
			}		
			{
				boost::lock_guard<boost::mutex> lock(m_mutOutQueueClose);
				m_mapOutQueueClose[this] = true;
			}
			m_condOutQueueClose.notify_all();
			boost::this_thread::interruption_point();
		}
		{
			boost::unique_lock<boost::mutex> lock(m_mutOutQueueClose);
			bool bClosePossible = false;
			
			for(;;)
			{
				bool btemp = true;
				for (auto v : m_mapOutQueueClose)
				{
					btemp &= v.second;
				}
				bClosePossible = btemp;
				if (bClosePossible)
					break;
				m_condOutQueueClose.wait(lock);				
			}
			m_outputQueue.close();
		}
	}
	catch (sync_queue_is_closed const&)
	{		
		m_outputQueue.close();
	}
	catch (runtime_error const& err)
	{
		{
			boost::lock_guard<boost::mutex> lock(m_mutThreads);
			m_bProcessingError = true;
		}
		src::severity_logger_mt< severity_level >& lg = app_logger::get();		
		BOOST_LOG_SEV(lg, logging::trivial::info) << err.what();
		// need to stop everything
		this->m_inputQueue.close();
		this->m_outputQueue.close();
		{
			boost::this_thread::disable_interruption di;
			boost::lock_guard<boost::mutex> lock(m_mutThreads);
			for (auto thread : m_aThreads)
			{	
				if (thread != nullptr)
				{
					thread->interrupt();
				}				
			}
		}
	}
}

bool TranslationThread::MakeLabels(string const& in, string& out)
{
	typedef tokenizer<char_separator<char> >  Tokenizer;
	char_separator<char> sep("\t");

	Tokenizer tok(in, sep);
	m_aSourceColumns.assign(tok.begin(), tok.end());

	string tempOut;
	for (string column : m_aSourceColumns)
	{
		auto it = m_mapColumns.find(column);
		if (it != m_mapColumns.cend())
		{
			m_aTargetColumns.push_back(it->second);
			tempOut += it->second + string("\t");
		}
	}
	out = tempOut;
	if (tempOut.length() == 0)
	{			
		throw runtime_error("Error during column names parsing");
	}

	{
		boost::lock_guard<boost::mutex> lock(m_mutColumnsInit);
		m_bColumnsInit = true;		
	}
	m_condColumnsInit.notify_all();

	return true;
}

bool TranslationThread::DecodeLine(string const& in, VendorData& out)
{
	typedef tokenizer<char_separator<char> >  Tokenizer;
	char_separator<char> sep("\t");

	Tokenizer tok(in, sep);
	vector< string > vec;
	vec.assign(tok.begin(), tok.end());
	if (vec.size() > 0)
	{
		unsigned int vSize = std::min(vec.size(), m_aSourceColumns.size());
		out.Identifier = vec[0];
		for (unsigned int i = 1; i < vSize; i++)
		{
			out.Values[m_aSourceColumns[i]] = vec[i];
		}		
	}
	return true;
}

bool TranslationThread::TranslateLine(VendorData const& in, VendorData& out)
{
	auto it = m_mapIdentifier.find(in.Identifier);
	if (it != m_mapIdentifier.cend())
	{
		out.Identifier = it->second;

		for (auto value : in.Values)
		{
			auto vIt = m_mapColumns.find(value.first);
			if (vIt != m_mapColumns.cend())
			{
				out.Values[vIt->second] = value.second;
			}
		}
		return true;
	}
	return false;
}

string TranslationThread::EncodeLine(VendorData const& in)
{
	string temp = in.Identifier;

	for (unsigned int i = 0; i < m_aTargetColumns.size(); i++)
	{
		string col = m_aTargetColumns[i];
		auto value = in.Values.find(col);
		if (value != in.Values.cend())
		{
			temp += "\t" + value->second;
		}
	}
	return temp;
}

bool TranslationThread::MakeOutputLine(Line const & in, string & out)
{
	if (in.Number == 0)
	{
		return MakeLabels(in.Content, out);
	}
	else
	{
		boost::unique_lock<boost::mutex> lock(m_mutColumnsInit);
		while (!m_bColumnsInit)
		{			
			m_condColumnsInit.wait(lock);		
		}

		VendorData dataIn;
		if (DecodeLine(in.Content, dataIn))
		{
			VendorData dataOut;
			if (TranslateLine(dataIn, dataOut))
			{
				out = EncodeLine(dataOut);
				return true;
			}
		}
		return false;
	}
}
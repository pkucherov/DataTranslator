#include "stdafx.h"
#include "DataAgent.h"

DataAgent::DataAgent(sync_bounded_queue<Line>& queue) :
	m_processingQueue(queue)
{
}

DataAgent::~DataAgent()
{

}
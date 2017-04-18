#include "stdafx.h"
#include "ProcessController.h"
#include <iostream>
using namespace std;

int main(int argc, char* argv[])
{
	if (argc < 2)
	{
		std::cout << "\nUsage: DataTranslator <folder>" << std::endl;
		return 1;
	}

	ProcessController controller(argv[1]);
	controller.Run();

	return 0;
}


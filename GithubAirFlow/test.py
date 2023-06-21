import sys


def ss():
	sys.path.insert(1,"gitTry/")

	from hello import helloWorld

	helloWorld()

ss()

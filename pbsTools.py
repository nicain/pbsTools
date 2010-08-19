#  pbsTools.py  hi234
#  Created by nicain on 11/11/09.
#  Copyright (c) 2009 __MyCompanyName__. All rights reserved.
#
################################################################################
# This function runs all of the subordinate functions in concert:
def runPBS(
	commandString, 
	includeIDAsArg = 0, 
	fileList = (), 
	callMake = 0, 
	dryRun = 1,
	runLocation = 'local', 
	runType = 'wallTimeEstimate',
	waitForSims = 1,
	wallTime = 30*60, 
	wallTimeEstCount = 5, 
	buildDir = './', 
	hiddenDir = './.submitDir', 
	outputDir = 'simResults', 
	queue = 'default',
	nodes = 'default', 
	ppn = 'default', 
	repspp = 'default',			# Probably never change
	verbose = 1,
	niceLevel = 20):

	##### Option for runLocation ######
	# local (default)
	# steele
	# abe
	# cluster

	##### Option for runType ######
	# wallTimeEstimate (default)
	# batch

	import os
	from subprocess import call as call
	from time import sleep

	# Check to make sure fileList is in fact a list:
	print fileList
	if not isinstance(fileList,(list,tuple)):
		print('  fileList is not a list! Making it a list with at least one element...')
		fileList = (fileList,)

	# Create dictionary of all settings:
	settings = {}
	settings['submitFileName'] = 'qsubber.csh'		# Probably never need to change
	settings['qSubFileName'] = 'qsubber.csh'		# Probably never need to change
	settings['PBSFileNamePrefix'] = 'currentNode_'	# Probably never need to change
	settings['slaveFileNamePrefix'] = 'slave_'		# Probably never need to change
	settings['PBSDir'] = 'PBSTemp'					# Probably never need to change
	settings['SlaveDir'] = 'SlaveTemp'				# Probably never need to change
	ppservers = ('pineapple','watermelon','peach','lime','grape','melon')#,'kitsap','peach','melon')			# Must be tuple, not list
	settings['clustServerList'] = ppservers

	settings['commandString'] = commandString
	settings['includeIDAsArg'] = includeIDAsArg
	settings['fileList'] = fileList
	settings['callMake'] = callMake	
	settings['dryRun'] = dryRun
	settings['runLocation'] = runLocation
	settings['runType'] = runType
	settings['waitForSims'] = waitForSims
	settings['wallTime'] = wallTime
	settings['wallTimeEstCount'] = wallTimeEstCount
	settings['buildDir'] = os.path.abspath(os.path.expanduser(buildDir))
	settings['hiddenDir'] = os.path.abspath(os.path.expanduser(hiddenDir))
	settings['outputDir'] = os.path.abspath(os.path.expanduser(outputDir))
	settings['verbose'] = verbose
	settings['niceLevel'] = niceLevel

	settings['cwd'] = os.getcwd()
	settings['qSubCommand'] = 'qsub -S /bin/tcsh -q '

	if runLocation == 'local':
		if runType == 'wallTimeEstimate':
			settings['nodes'] = 1
			settings['ppn'] = 1
			settings['repspp'] = 1
			settings['qSubCommand'] = settings['qSubCommand'] + 'LOCAL WALLTIMEEST '
			settings['server'] = 'LOCAL WALLTIMEEST'
			settings['wallTime'] = 30*60
		elif runType == 'batch':
			if nodes == 'default': settings['nodes'] = 1
			else: settings['nodes'] = nodes
			if ppn == 'default': settings['ppn']=1
			else: settings['ppn'] = ppn
			if repspp == 'default': settings['repspp']=1
			else: settings['repspp'] = repspp
			settings['qSubCommand'] = 'LOCAL BATCH'
			settings['server'] = 'LOCAL BATCH'
			
	elif runLocation == 'cluster':
		if runType == 'wallTimeEstimate':
			settings['nodes'] = 1
			settings['ppn'] = 1
			settings['repspp'] = 1
			settings['qSubCommand'] = settings['qSubCommand'] + 'CLUSTER WALLTIMEEST '
			settings['server'] = 'CLUSTER WALLTIMEEST'
			settings['wallTime'] = 30*60
			settings['waitForSims'] = 1
		elif runType == 'batch':
			if nodes == 'default': settings['nodes'] = 1
			else: settings['nodes'] = nodes
			if ppn == 'default': settings['ppn']=1
			else: settings['ppn'] = ppn
			if repspp == 'default': settings['repspp']=1
			else: settings['repspp'] = repspp
			settings['qSubCommand'] = 'CLUSTER BATCH'
			settings['server'] = 'CLUSTER BATCH'
			settings['waitForSims'] = 1

	elif runLocation == 'abe':
		if runType == 'wallTimeEstimate':
			settings['nodes'] = 1
			settings['ppn'] = 1
			settings['repspp'] = 1
			if queue == 'default': settings['queue'] = 'debug'
			else: settings['queue'] = queue
			settings['qSubCommand'] = settings['qSubCommand'] + settings['queue'] + ' '
			settings['server'] = 'wallTimeEstimate'
		elif runType == 'batch':
			if nodes == 'default': settings['nodes'] = 1
			else: settings['nodes'] = nodes
			if ppn == 'default': settings['ppn']=8
			else: settings['ppn'] = ppn
			if repspp == 'default': settings['repspp']=1
			else: settings['repspp'] = repspp
			if queue == 'default': settings['queue'] = 'normal'
			else: settings['queue'] = queue
			settings['qSubCommand'] = settings['qSubCommand'] + settings['queue'] + ' '
			settings['server'] = 'normal'

	elif runLocation == 'steele':
		if runType == 'wallTimeEstimate':
			settings['nodes'] = 1
			settings['ppn'] = 1
			settings['repspp'] = 1
			if queue == 'default': settings['queue'] = 'tg_workq'
			else: settings['queue'] = queue
			settings['qSubCommand'] = settings['qSubCommand'] + settings['queue'] + ' '
			settings['server'] = 'wallTimeEstimate'
		elif runType == 'batch':
			if nodes == 'default': settings['nodes'] = 1
			else: settings['nodes'] = nodes
			if ppn == 'default': settings['ppn']=8
			else: settings['ppn'] = ppn
			if repspp == 'default': settings['repspp']=1
			else: settings['repspp'] = repspp
			if queue == 'default': settings['queue'] = 'tg_workq'
			else: settings['queue'] = queue
			settings['qSubCommand'] = settings['qSubCommand'] + settings['queue'] + ' '
			settings['server'] = 'normal'

	else:
		print 'Invalid Teragrid server type: ',runLocation,'; exiting...'
		import sys
		sys.exit()
		
	if settings['verbose']:
		displaySettings(settings, continuePrompt = 1)

	if callMake == 1:
		os.chdir(settings['buildDir'])
		call('make',shell=True)
		os.chdir(settings['cwd'])

	createJobDirs(settings)
	makeSubmissionFiles(settings)
	copyFiles(settings)

	if dryRun == 1:
		print 'Dryrunning; Command to be called: ' + os.path.join(settings['hiddenDir'],settings['qSubFileName'])
		if settings['verbose']:
			userInput = raw_input("  Press <return> to continue...")
	else:
		if runLocation == 'local':
			print 'Local run mode selected.'
			if settings['verbose']:
				userInput = raw_input("  Press <return> to continue...")
			if settings['runType'] == 'wallTimeEstimate':
				os.chdir(os.path.join(settings['hiddenDir'], 'currJob_1_' + str(settings['repspp'])))
				call('python wallTimeEst.py',shell=True)
				os.chdir(settings['cwd'])
			elif settings['runType'] == 'batch':
				for i in range(1,settings['nodes']*settings['ppn']*settings['repspp']+1):
					call(os.path.join(settings['hiddenDir'], 'currJob_' + str(i),settings['slaveFileNamePrefix'] + str(i) + '.csh'),shell=True)
			else:
				print 'Invalid runType : ',runType,'; exiting...'
				import sys
				sys.exit()

		elif runLocation == 'steele' or runLocation == 'abe':
			os.system(os.path.join(settings['hiddenDir'],settings['qSubFileName']))			
			if waitForSims == 1:
				waitForJobs(settings)
		
		elif runLocation == 'cluster':
			
			# Ill need this:
			import pp
			
			# This is the function that runs each job, over the shared file system:
			def doTheMagic(where, fileName, niceLevel):
				#subprocess.call('nice -n ' + str(niceLevel) +' ' + os.path.join(where,fileName),shell=True,cwd=where)
				time.sleep(20)
				return 0
			
			# Gather names and directories of all jobs:
			jobList=[]
			if settings['runType'] == 'wallTimeEstimate':
				
				# In this case, only one file to run:
				currLocation = os.path.join(settings['hiddenDir'], 'currJob_1_' + str(settings['repspp']))
				currName = 'wallTimeEst.py'
				jobList.append((currLocation, currName))
				
			elif settings['runType'] == 'batch':
				
				# In this case, create a list of each file to be run:
				for i in range(1,settings['nodes']*settings['ppn']*settings['repspp']+1):
					currLocation = os.path.join(settings['hiddenDir'], 'currJob_' + str(i))
					currName = settings['slaveFileNamePrefix'] + str(i) + '.csh'
					jobList.append((currLocation, currName, i))
				
			else:
				print 'Invalid runType : ',runType,'; exiting...'
				import sys
				sys.exit()
			
			# Start up servers:
			pauseTime = 5
			print 'Cluster run mode selected.'
			print '  Starting up servers...'
			passwd, deadTime = startServers(settings['clustServerList'])
			sleep(pauseTime)
			print '    Done.'
			
			# Connect to servers:
			print '  Connecting to servers...'
			job_server = pp.Server(ppservers=ppservers, secret = passwd, ncpus=0)
			print '    Done.'
			sleep(5) # God damn.  This took forever to figure out that I needed it...
			activeServers = job_server.get_active_nodes()
			totalCPUs = 0
			print '  Servers: '
			for server in activeServers.keys():
				totalCPUs = totalCPUs + activeServers[server]
				if server is not 'local':
					if server.count(':') > 0:
						serverNice = server.split(':')[0]
					else:
						serverNice = server
					print '    ' + serverNice + ': ' + str(activeServers[server])

			if totalCPUs == 0:
				print '    OOPS! All servers are busy! Running on local machine only ...'
				job_server.set_ncpus(ncpus='autodetect')
				sleep(1) # God damn.  This took forever to figure out that I needed it...
				activeServers = job_server.get_active_nodes()
				print '  Servers: ' + str(activeServers)[1:-1]
			
			if settings['verbose']:
				userInput = raw_input("  Press <return> to continue... ("+str(deadTime-pauseTime) +" seconds until server inactivity shutdown)")
				
			# Farm out the jobs to the server:
			counter = 0
			jobs=[0]*len(jobList)
			for input in jobList:
#				jobs[counter] = job_server.submit(doTheMagic,(input[0],input[1],niceLevel), (), ("subprocess","os"))
				jobs[counter] = job_server.submit(doTheMagic,(input[0],input[1],niceLevel), (), ("time",))
				print '    Job '+str(counter+1)+' submitted... (nice = ' + str(niceLevel) + ')' 
				counter += 1
		
			# Wait for the jobs:  (Note: this is forced for now... might change later... )
			if waitForSims == 1:
				waitForJobs(settings, job_server=job_server)
				
			# When done, print stats and close out:
			job_server.print_stats()
			job_server.destroy()
			

		else:
			print 'Invalid runLocation : ',runLocation,'; exiting...'
			import sys
			sys.exit()

		if waitForSims == 1:
			print '  Collecting results:'
			collectJobs(settings)

			print '  Deleting temporary files:'
			nukeDirs(settings['hiddenDir'])

			# Either local or not, if we did a wallTimeEst, display results:
			if settings['runType'] == 'wallTimeEstimate':
				print "********************************"
				print "* Wall-Time Estimate, each Processor:"
				print "********************************"
				for file in getFileIterator(settings['outputDir'], 'wallTimeEstData.dat'):
					scratch = call('cat ' + file, shell=True)

	return settings

################################################################################
# This function collects all of the job outputs into a single place:
def collectJobs(settings):
	import os, uuid, shutil, glob

	# Get rid of job controlling scripts; won't need those!
	nukeDirs(os.path.join(settings['hiddenDir'],settings['PBSDir']))
	os.remove(os.path.join(settings['hiddenDir'],settings['qSubFileName']))

	# Grab all the files generated by the processing; exclude symbolic links.
	for root, dirs, files in os.walk(settings['hiddenDir']):
		for currDir in dirs:
			fullCurrDir = os.path.join(root,currDir)
			for newRoot, newDirs, files in os.walk(fullCurrDir):
				for file in files:
					if not os.path.islink(os.path.abspath(os.path.join(newRoot,file))) and not file == 'jobCompleted':
						shutil.copyfile(os.path.abspath(os.path.join(newRoot,file)),os.path.join(settings['outputDir'],'currJob_'+file+'_'+str(uuid.uuid4())))

	# Grab error and output logs for later use.
	for file in glob.glob(os.path.join(settings['cwd'],settings['PBSFileNamePrefix'] + '*.[eo]*')):
		shutil.move(file, settings['outputDir'])

	return 0

################################################################################
# This function deletes a directory:
def nukeDirs(deleteDir):
	import shutil

	shutil.rmtree(deleteDir,ignore_errors=True)

	return 0


################################################################################
# This function displays the current settings of the job:
def displaySettings(settings, continuePrompt = 1):
	import os

	# Create a special settings banner:
	banner = ''
	if settings['dryRun'] == 1: 
		banner = banner + 'DRYRUN '
	if settings['runLocation'] == 'local': 
		banner = banner + 'LOCALRUN '
	elif settings['runLocation'] == 'cluster': 
		banner = banner + 'CLUSTERRUN '
	if settings['runType'] == 'wallTimeEstimate': 
		banner = banner + 'WALLTIMEESTIMATE '

	print "****************************************************************"
	print " Teragrid PBS job ready to run: " + banner	
	print "****************************************************************"
	print " "
	print "Job Details:"
	print "  Server: " + settings['server']
	print "  Compile with make: " + str(settings['callMake'])
	print "  Walltime (seconds): " + str(settings['wallTime'])
	print "  Nodes: " + str(settings['nodes'])
	print "  Processors Per Node (PPN): " + str(settings['ppn'])
	print "  Simulations Per Processor (repsPP): " + str(settings['repspp'])
	print "  Wait for Sims: " + str(settings['waitForSims'])
	print " "
	print "Build Details:"	
	print "  Build Directory: " + settings['buildDir']
	print "  Output Directory: " + settings['outputDir']
	print "  Hidden Directory: " + settings['hiddenDir']
	print " "
	print "Run Details:"
	if settings['includeIDAsArg'] == 0:
		print "  Command: " + settings['commandString']
	else:
		print "  Command: " + settings['commandString'] + ' $ID'
	print "  Files used: "
	for i in range(len(settings['fileList'])):
		print "    " + settings['fileList'][i]
	print "  Total Sims: " + str(settings['nodes']*settings['ppn']*settings['repspp'])


	if continuePrompt==1:
		userInput = raw_input("Press return to continue, or \'C\' to cancel: ")
		if userInput.upper()=='C':
			import sys
			sys.exit()

	return 0

################################################################################
# This function pauses the script until all teragrid jobs are done:
def waitForJobs(settings,job_server = 0):

	import time, os
	import progressMeter as pm
	
	if (settings['runLocation'] == 'abe') or (settings['runLocation'] == 'steele'):
		sleepTime=20
	else:
		sleepTime=3

	breakout=0
	maxJobs = settings['nodes']*settings['ppn']*settings['repspp']
	p = pm.ProgressMeter(total=maxJobs+1)
	p.update(1)

	numberCompleted = 0
	while breakout !=1:
		time.sleep(sleepTime)
		if not (type(job_server).__name__ == 'int'):
			job_server.print_stats()

		# Check each job directory for the standard out file:
		oldNumberCompleted = numberCompleted
		numberCompleted = 0
		for root, dirs, files in os.walk(settings['hiddenDir']):
			for currDir in dirs:
				if os.path.isfile(os.path.join(root,currDir,'jobCompleted')):
					numberCompleted += 1

		numberCompletedThisRound = numberCompleted - oldNumberCompleted
		if numberCompletedThisRound > 0:
				p.update(numberCompletedThisRound)

		if numberCompleted == maxJobs:
			breakout = 1

	return

################################################################################
# This function creates a sequence of job directories for the pbs script:
def createJobDirs(settings):
	import os

	print '  Creating hidden directories and files:'  

	# Check for an allowable hidden directory:
	breakout = False	
	while breakout != True:
		if os.path.isdir(settings['hiddenDir']):
			print '    Hidden directory already exist: ' + settings['hiddenDir']
			if settings['verbose']:
				userInput = raw_input("    Press \'Q\' to exit, \'O\' to overwrite, or enter a new directory name: ")
				if userInput.upper()=='O':
					nukeDirs(settings['hiddenDir'])
					os.makedirs(settings['hiddenDir'])
					breakout = True
				elif userInput.upper()=='Q':
					print '    Aborting run...'
					import sys
					sys.exit()
				elif userInput == '':
					breakout = False
				else:
					settings['hiddenDir'] = userInput
			elif not settings['verbose']:
				print '		Overwriting hidden directory'
				nukeDirs(settings['hiddenDir'])
				os.makedirs(settings['hiddenDir'])
				breakout = True
			else:
				print '		Invalid verbose option specified... aborting'
				import sys
				sys.exit()
		else:
			os.makedirs(settings['hiddenDir'])
			breakout = True

	# Check for an allowable output directory:
	breakout = False	
	while breakout != True:
		if os.path.isdir(settings['outputDir']):
			print '    Output directory already exist: ' + settings['outputDir']
			if settings['verbose']:
				userInput = raw_input("    Press \'Q\' to exit, \'O\' to overwrite, or enter a new directory name: ")
				if userInput.upper()=='O':
					nukeDirs(settings['outputDir'])
					os.makedirs(settings['outputDir'])
					breakout = True
				elif userInput.upper()=='Q':
					print '    Aborting run...'
					nukeDirs(settings['hiddenDir'])
					import sys
					sys.exit()
				elif userInput == '':
					breakout = False
				else:
					settings['outputDir'] = userInput
			elif not settings['verbose']:
				print '		Overwriting output directory'
				nukeDirs(settings['outputDir'])
				os.makedirs(settings['outputDir'])
				breakout = True
			else:
				print '		Invalid verbose option specified... aborting'
				nukeDirs(settings['hiddenDir'])
				import sys
				sys.exit()
		else:
			os.makedirs(settings['outputDir'])
			breakout = True


	os.mkdir(os.path.join(settings['hiddenDir'], settings['PBSDir']))

	# Run a loop to create subordinate run directories:
	if settings['runType'] == 'wallTimeEstimate':
		os.mkdir(os.path.join(settings['hiddenDir'], 'currJob_1_' + str(settings['repspp'])))
	else:
		for i in range(1,settings['nodes']*settings['ppn']*settings['repspp']+1):
			os.mkdir(os.path.join(settings['hiddenDir'], 'currJob_' + str(i)))

	return 0

################################################################################
# This function writes the job submission files:
def makeSubmissionFiles(settings):
	import os, time

	# Write qsubber file:
	qsubber = open(os.path.join(settings['hiddenDir'],settings['qSubFileName']), 'w')
	if settings['server']=='debug':
		qsubber.write(settings['qSubCommand'] + ' -I -l walltime=' + \
				      GetInHMS(settings['wallTime']) + \
				      ',nodes=' + str(settings['nodes']) + ':ppn=' + str(settings['ppn']))
	else:
		for i in range(1,settings['nodes']+1):
			qsubber.write(settings['qSubCommand'] + os.path.join(settings['hiddenDir'], settings['PBSDir'], settings['PBSFileNamePrefix'] + str(i)) + '.pbs\n')
	qsubber.close()
	os.system("chmod +x " + os.path.join(settings['hiddenDir'],settings['qSubFileName']))

	# Write currentNode_#.pbs files:
	counter=0
	for i in range(1,settings['nodes']+1):
		currentPBSFileName = os.path.join(settings['hiddenDir'], settings['PBSDir'], settings['PBSFileNamePrefix'] + str(i)) + '.pbs'
		currentNoder=open(currentPBSFileName, 'w')
		currentNoder.write('#PBS -l walltime=' + GetInHMS(settings['wallTime']) + '\n')
		if settings['runLocation'] == 'abe' or settings['runLocation'] == 'local':
			currentNoder.write('#PBS -l nodes=1:ppn=' + str(settings['ppn']) + '\n')
		elif settings['runLocation'] == 'steele':
			currentNoder.write('#PBS -l select=1:ncpus=' + str(settings['ppn']) + '\n')
		currentNoder.write('set NP=`wc -l $PBS_NODEFILE | cut -d\'/\' -f1`' + '\n')
		currentNoder.write('set JOBID=`echo $PBS_JOBID | cut -d\'.\' -f1`' + '\n')
		for j in range(1,settings['ppn']+1):
			if settings['runType'] == 'wallTimeEstimate':
				counter=counter+1
				currentNoder.write('cd ' + os.path.join(settings['hiddenDir'], 'currJob_1_' + str(settings['repspp'])) + '\n')
				currentNoder.write('python wallTimeEst.py & \n')
			else:
				for k in range(1,settings['repspp']+1):
					counter=counter+1
					currentNoder.write(   os.path.join(settings['hiddenDir'], 'currJob_' + str(counter), settings['slaveFileNamePrefix'] + str(counter) + '.csh') + ' &' + '\n')
		currentNoder.write('wait' + '\n')
		currentNoder.close()
		os.system('chmod +x ' + currentPBSFileName)

	# In wallTimeEstimate mode, write wallTimeEst.py file:
	if settings['runType'] == 'wallTimeEstimate':
		currentFileName = os.path.join(settings['hiddenDir'], 'currJob_1_' + str(settings['repspp']),'wallTimeEst.py')
		currentFile=open(currentFileName, 'w')

		currentFile.write('#!/usr/bin/env python\n')
		currentFile.write('\n')
		currentFile.write('import timeit\n')
		currentFile.write('import pbsTools as pt\n')
		currentFile.write('numberOfTrials=' + str(settings['wallTimeEstCount']) + '\n')
		currentFile.write('repspp=' + str(settings['repspp']) + '\n')
		currentFile.write('totalTime=timeit.Timer(\'os.system(\"' + os.path.join(settings['hiddenDir'], 'currJob_1_' + str(settings['repspp']), settings['slaveFileNamePrefix'] + str(1) + '.csh') + '\")\',\'import os\').repeat(*[numberOfTrials,repspp])\n')
		currentFile.write('myMean = pt.mean(totalTime)\n')
		currentFile.write('myStddev = pt.stddev(totalTime)\n')
		currentFile.write('f = open(\'wallTimeEstData.dat\', \'w\')\n')
		currentFile.write('print >> f, \"Mean: \", myMean\n')
		currentFile.write('print >> f, \"Standard Deviation: \", myStddev\n')
		currentFile.write('print >> f, \"Suggested wallTime: \", myMean+4*myStddev\n')
		currentFile.write('f.close\n')
		currentFile.write('\n')
		currentFile.write('from subprocess import call as call\n')
		currentFile.write('call("touch jobCompleted", shell=True)')
		currentFile.close()
		
		os.system('chmod +x ' + currentFileName)

	# Write slave_#.csh files
	if settings['runType'] == 'wallTimeEstimate':
		iterMax=1
	else:
		iterMax=settings['nodes']*settings['ppn']*settings['repspp']

	for i in range(1,iterMax+1):
		if settings['runType'] == 'wallTimeEstimate':
			currentSlaveDir = os.path.join(settings['hiddenDir'], 'currJob_1_' + str(settings['repspp']))
		else:
			currentSlaveDir = os.path.join(settings['hiddenDir'], 'currJob_' + str(i))

		currentSlaveFileName = os.path.join(currentSlaveDir, settings['slaveFileNamePrefix'] + str(i) + '.csh')
		currentSlaver=open(currentSlaveFileName, 'w')
		currentSlaver.write('cd ' + currentSlaveDir + '\n')
		if settings['includeIDAsArg'] == 0:
			currentSlaver.write(settings['commandString'] + '\n')
		else:
			currentSlaver.write(settings['commandString'] + ' ' + str(i) + '\n')
		if settings['runType'] != 'wallTimeEstimate':
			currentSlaver.write('touch jobCompleted')
		currentSlaver.close()
		os.system('chmod +x ' + currentSlaveFileName)

	return 0

################################################################################
# This function copies relevent source files to the submission directories:
def copyFiles(settings):
	import shutil, os

	for file in settings['fileList']:
		(currDir, currFile) = os.path.split(file)
		if currDir == '':
			sourceDir = settings['buildDir']
		elif currDir == '.':
			sourceDir = settings['cwd']
		else:
			sourceDir = currDir
		if settings['runType'] == 'wallTimeEstimate':
			os.symlink(os.path.join(sourceDir,file),os.path.join(settings['hiddenDir'], 'currJob_1_' + str(settings['repspp']),currFile))
		else:
			for i in range(1,settings['nodes']*settings['ppn']*settings['repspp']+1):
				os.symlink(os.path.join(sourceDir,file),os.path.join(settings['hiddenDir'], 'currJob_' + str(i),currFile))

################################################################################
# This function creates settings file based on a parameter sweep dictionary
def makeSettingsFile(paramDict, npp, fileName='settingsFile.dat',writeDir='./', fIDVarName='fileID'):
	import os

	def product(*args, **kwds):
		# product('ABCD', 'xy') --> Ax Ay Bx By Cx Cy Dx Dy
		# product(range(2), repeat=3) --> 000 001 010 011 100 101 110 111
		pools = map(tuple, args) * kwds.get('repeat', 1)
		result = [[]]
		for pool in pools:
			result = [x+[y] for x in result for y in pool]
		for prod in result:
			yield tuple(prod)

	# Create Settings iterator from dictionary:
	params = paramDict.keys()
	params.sort()
	settingsList = []
	for parameter in params: 
		settingsList.append(paramDict[parameter])
	settingsIterator = product(*settingsList)

	# Create list of names to write out:
	nameList = [param + "=" for param in params]

	# Write settings file:
	fOut = open(os.path.join(os.path.abspath(os.path.expanduser(writeDir)),fileName),'w')
	print >> fOut, 'import sys'
	print >> fOut, ''
	counter = 0
	for currentSettings in settingsIterator:
		for i in range(1,npp+1):
			counter = counter + 1
			currentSettingsStr = map(str,currentSettings)
			toWriteLineList = map(''.join,zip(nameList,currentSettingsStr))
			print >> fOut, 'if int(sys._getframe(1).f_locals[\'' + fIDVarName + '\']) == ' + str(counter) + ':'
			for line in toWriteLineList:
				print >> fOut, '	' + line
			print >> fOut, ''

	fOut.close()

	return counter

################################################################################
# This function creates a file iterator based on an input string:
def getFileIterator(myDir, fileString):
	import glob, os

	return glob.glob(os.path.join(os.path.abspath(os.path.expanduser(myDir)),'*' + fileString + '*'))

################################################################################
# This function loads settings from settings file based on uniqueID
def pickle(myVars, saveFileName = 'simResults.dat'):
	import pickle as pickleModule

	fOut = open(saveFileName,'w')
	pickleModule.dump(myVars,fOut)
	fOut.close()	
	return

################################################################################
# This function loads settings from settings file based on uniqueID
def unpickle(saveFileName = 'simResults.dat'):
	import pickle as pickleModule

	fIn = open(saveFileName,'r')
	myPickle = pickleModule.load(fIn)
	fIn.close()	
	return myPickle

################################################################################
# This function gets saved variables from a list of similarly named files in a directory:
def getFromPickleJar(loadDir = 'simResults', fileNameSubString = 'simResults.dat'): 
	import pickle

	# Get file iterator:
	fileIterator = getFileIterator(loadDir, fileNameSubString)

	# Set up output array:
	resultList = [0]*len(fileIterator)

	# Import values into a list:
	counter = 0
	for myFile in fileIterator:
		fIn = open(myFile)
		resultList[counter] =  pickle.load(fIn)
		counter += 1

	return resultList

################################################################################
# For wall time est use:
def mean(values):
	"""Return the arithmetic average of the values."""
	return sum(values) / float(len(values))

def stddev(values, meanval=None):
	"""The standard deviation of a set of values.
	Pass in the mean if you already know it."""
	import math

	if meanval == None: meanval = mean(values)
	return math.sqrt(sum([(x - meanval)**2 for x in values]) / (len(values)-1))

################################################################################
# In case of a broken job, to recover to a directory:
def brokenJobRecovery(buildDir = './', hiddenDir = './.submitDir', outputDir = 'simResults'):
	import os

	# Create Settings Dictionary; start with imports:
	settings = {}
	settings['buildDir'] = os.path.abspath(os.path.expanduser(buildDir))
	settings['hiddenDir'] = os.path.abspath(os.path.expanduser(hiddenDir))
	settings['outputDir'] = os.path.abspath(os.path.expanduser(outputDir))


	# Definitions, should be same as in runPBS:
	settings['cwd'] = os.path.abspath(os.path.expanduser(os.getcwd()))
	settings['qSubFileName'] = 'qsubber.csh'
	settings['PBSDir'] = 'PBSTemp'
	settings['PBSFileNamePrefix'] = 'currentNode_'

	# Recover the job:
	print '  Collecting results:'
	collectJobs(settings)

	print '  Deleting temporary files:'
#	nukeDirs(settings['hiddenDir'])

	return

################################################################################
# Convert seconds to a HMS string:
def GetInHMS(seconds):
	hours = seconds / 3600
	seconds -= 3600*hours
	minutes = seconds / 60
	seconds -= 60*minutes
	return "%02d:%02d:%02d" % (hours, minutes, seconds)

################################################################################
# Start up cluster servers:	
def startServers(serverList):

	# Import necessary packages:
	import subprocess as sp
	import random
	import time
	
	# Set a timeout, to kill servers if nothing connects/after completion:
	deadTime = 60
	
	# Set up a password, to keep this job unique:
	passwd = str(random.randint(10000,99999))
	
	# Define useful sub-functions:
	def check_output(input, getReturn = 1):
		if getReturn == 1:
			return sp.Popen(input,stdout=sp.PIPE,stdin=sp.PIPE,shell=True).communicate()
		else:
			return sp.Popen(input,stdin=sp.PIPE,shell=True).communicate()
			
	def sshCallReturn(command,server, getReturn = 1, background=0):
		sshCommand = 'ssh ' + server + ' \'' + command + '\''
		if background == 1:
			sshCommand = sshCommand + ' &'
		return check_output(sshCommand, getReturn = getReturn)

	def getCurrLoad(server):
		command = "sar | tail -n 2 | head -n 1"	
		output = sshCallReturn(command, server)[0].strip().split()
		totalLoad = float(output[-1])
		return totalLoad/100

	def getNumCurrAvailProc(server):
		command = "cat /proc/cpuinfo | grep processor | wc -l"
		output = sshCallReturn(command, server)[0].strip().split()
		nCPU = float(output[0])
		totalLoad = getCurrLoad(server)
		nAvailCPU = round(nCPU*totalLoad)
		return int(nAvailCPU)

	# Query server availibility, and start up the servers:
	for server in serverList:
		currNumCPU = str(getNumCurrAvailProc(server))
		print '    Starting ' + server 
		command = 'nohup ppserver.py -d -w '+currNumCPU+' -t '+str(deadTime)+' -s '+passwd+' > ~/local/logs/'+server+'_debug.log 2>&1 &'
		sshCallReturn(command, server, getReturn=0, background=1)
	
	return (passwd, deadTime)
		
		
		
		
		
		
		
		
		




#!/usr/bin/python3
import sys
import re

from pyspark import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeans, KMeansModel, StreamingKMeans
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col

import operator
import json
import math

sc = SparkContext(appName="test")
ssc = StreamingContext(sc, 5)
sqlContext = SQLContext(sc)

spark = SparkSession.builder.appName('fpl').getOrCreate()

playerProfile = spark.read.csv('players.csv' ,header=True)
playerProfile = playerProfile.withColumn("Goals", lit(0))
playerProfile = playerProfile.withColumn("Own Goals", lit(0))
playerProfile = playerProfile.withColumn("Fouls", lit(0))
playerProfile = playerProfile.withColumn("Shots on Target", lit(0))
playerProfile = playerProfile.withColumn("Pass Accuracy", lit(0))
playerProfile = playerProfile.withColumn("Matches", lit(0))

lines = ssc.socketTextStream("localhost",6100)

lines.pprint()

'''
def passAccCalc(df1, dict1):
	passes=df1.filter(df1['eventId']==8).orderBy(df1.playerId)
	accurate_passes=0
	accurate_key_passes=0
	total_passes=0
	total_key_passes=0
	passes_list=[]
	passes = passes.select('playerId', 'tags')
	passes.show()
	passes.printSchema()
	#passes_rdd = passes.select('tags').rdd.map(lambda x:x.tags).map(lambda x:x[0])
	#passes_rdd = passes.select('tags').rdd.flatMap(lambda x: [y[0] for y in x])
	#passes_rdd = passes.rdd.map(lambda x:1 if (x.id==1801) else 2 if (x.id==1802) else 3 if (x.id==302) else 0)
	#passes_rdd = passes.select('tags').rdd.reduce(lambda x, y: print(y.tags))
	#passes = passes.withColumn("tags", map(lambda x, y: x+y, passes.tags))
	#passes.withColumn("tags", expr("transform(tags, x -> array(x.*))")).select(flatten("tags")).show()
	#passes = spark.createDataFrame(passes_rdd)

		
	passes = passes.select(passes.playerId, explode(passes.tags))
	passes = passes.groupBy("playerId","col").count()
	passes = passes.filter((passes.col.id==1802) | (passes.col.id==302) | (passes.col.id==1801) )
	

	
	passes = passes.groupBy("playerId","col").count()
	col_as_int = passes.select(passes.col).rdd.map(lambda x:x[0])
	col_as_int = spark.createDataFrame(col_as_int)
	
	passes = passes.withColumn("id1",monotonically_increasing_id())
	col_as_int = col_as_int.withColumn("id2",monotonically_increasing_id())
	
	passes = passes.join(col_as_int, passes.id1==col_as_int.id2)
	passes = passes.select('playerId','id','count')
	passes = passes.withColumn("id", passes.id.cast(IntegerType()))
	passes = passes.filter(passes["id"]==1802 | passes["id"]==1801 | passes["id"]==302)
	
	#passes.printSchema()
	#passes.show()
	#col_as_int.show()

'''

def passAccCalc(df1, dict1):
	global playerProfile
	passes=df1.filter(df1['eventId']==8).orderBy(df1.playerId)
	#playerProfile.printSchema()
	accurate_passes=0
	accurate_key_passes=0
	total_passes=0
	total_key_passes=0
	passes_list=[]
	passes = passes.select('playerId', 'tags')
	curr = passes.collect()[0].playerId
	for row in passes.collect():
		if row.playerId!=curr:
			pass_accuracy = (accurate_passes + (accurate_key_passes * 2))/(total_passes + (total_key_passes * 2))
			#playerProfile = playerProfile.withColumn("Pass Accuracy", when(playerProfile["Id"]==curr, ( (playerProfile["Pass Accuracy"]+pass_accuracy)/playerProfile["Matches"] )))
			print("\nPass Accuracy for player ", row.playerId ," is ", pass_accuracy)	
			dict1[curr]=pass_accuracy			
			curr = row.playerId
			accurate_passes=0
			accurate_key_passes=0
			total_passes=0
			total_key_passes=0	
		if row.playerId==curr:
			tagslist=row.tags
			for i in tagslist:
				passes_list.append(i.id)
			if 1801 in passes_list and 302 in passes_list:
				accurate_key_passes+=1
				total_key_passes+=1
			if 1801 in passes_list and 302 not in passes_list:
				accurate_passes+=1
				total_passes+=1
			if 1802 in passes_list and 302 in passes_list:
				total_key_passes+=1
			if 1802 in passes_list and 302 not in passes_list:
				total_passes+=1
		passes_list=[]	
	pass_accuracy = (accurate_passes + (accurate_key_passes * 2))/(total_passes + (total_key_passes * 2))
	print("\nPass Accuracy for player ", curr ," is ", pass_accuracy)	
	dict1[curr]=pass_accuracy	
	return dict1


def duelEffCalc(df1, dict1):
	duels = df1.filter(df1['eventId']==1).orderBy(df1.playerId)
	duels = duels.select('playerId', 'tags')
	curr = duels.collect()[0].playerId
	duels_won=0
	duels_tot=0
	duels_neut=0
	for row in duels.collect():
		if row.playerId!=curr:
			
			if duels_tot!=0:
				duels_eff=(duels_won+(duels_neut/2))/duels_tot
			else:
				duels_eff=0
			print("\nDuel Efficiency of player ",curr," is ",duels_eff)
			dict1[curr]=duels_eff
			duels_won=0
			duels_tot=0
			duels_neut=0
			curr=row.playerId
		else:
			duels_list=[]
			tagslist=row.tags
			for i in tagslist:
				duels_list.append(i.id)
			if 701 in duels_list:
				duels_tot+=1
			if 702 in duels_list:
				duels_neut+=1
				duels_tot+=1
			if 703 in duels_list:
				duels_won+=1
				duels_tot+=1    
	if duels_tot!=0:
		duels_eff=(duels_won+(duels_neut/2))/duels_tot
	else:
		duels_eff=0
	print("\nDuel Efficiency of player ",curr," is ",duels_eff)
	dict1[curr]=duels_eff
	return dict1
	
def FKEff(df1, dict1):
	freekick =df1.filter(df1['eventId']==3).orderBy(df1.playerId)
	effective_freekick=0
	penalties_scored=0

	total_freekicks=0
	freekick_list=[]
	freekick = freekick.select('playerId', 'tags','subEventId')
	curr = freekick.collect()[0].playerId

	for row in freekick.collect():
		if row.playerId!=curr:
			
			if(total_freekicks !=0):
				free_kick_effectiveness = (effective_freekick+penalties_scored)/total_freekicks
			else:
				free_kick_effectiveness = 0

			print('\nThe Freekick Effectivness of a player ',curr,' is ',free_kick_effectiveness)
			dict1[curr]=free_kick_effectiveness
			curr = row.playerId
			effective_freekick=0
			penalties_scored=0

			total_freekicks=0

		if row.playerId==curr:
			tagslist=row.tags
			for i in tagslist:
				freekick_list.append(i.id)

			if 1801 in freekick_list and row.subEventId == 35:
				if 101 in freekick_list:
					penalties_scored+=1
				effective_freekick+=1
				total_freekicks+=1

			if 1801 in freekick_list and row.subEventId != 35:
				effective_freekick+=1
				total_freekicks+=1

			if 1802 in freekick_list and row.subEventId == 35:
				total_freekicks+=1

			if 1802 in freekick_list and row.subEventId != 35:
				total_freekicks+=1

		freekick_list=[]
	if(total_freekicks !=0):
		free_kick_effectiveness = (effective_freekick+penalties_scored)/total_freekicks
	else:
		free_kick_effectiveness = 0
	print('\nThe Freekick Effectivness of a player ',curr,' is ',free_kick_effectiveness)
	dict1[curr]=free_kick_effectiveness
	return dict1
	
def shotEff(df1, dict1):
	global playerProfile
	shots = df1.filter(df1['eventId']==10).orderBy(df1.playerId)
	shots = shots.select('playerId', 'tags')
	curr = shots.collect()[0].playerId
	shots_tot=0
	shots_goals=0
	shots_not_goal=0
	for row in shots.collect():
		if row.playerId!=curr:
			if shots_tot!=0:
				shots_eff=(shots_goals+(shots_not_goal/2))/shots_tot
			else:
				shots_eff=0
			print("\nShot Effectiveness of player ",curr," is ",shots_eff)
			dict1[curr]=shots_eff
			shots_tot=0
			shots_goals=0
			shots_not_goal=0
			curr=row.playerId
		else:
			shots_list=[]
			tagslist=row.tags
			for i in tagslist:
				shots_list.append(i.id)
			if 1801 in shots_list:
				playerProfile = playerProfile.withColumn("Shots on Target", when(playerProfile["Id"]==curr, playerProfile["Shots on Target"]+1))
				shots_tot+=1
				if 101 in shots_list:
					shots_goals+=1
					playerProfile = playerProfile.withColumn("Goals", when(playerProfile["Id"]==curr, playerProfile["Goals"]+1))
				else:
					shots_not_goal+=1
			if 1802 in shots_list:
				shots_tot+=1
				
	if shots_tot!=0:
		shots_eff=(shots_goals+(shots_not_goal/2))/shots_tot
	else:
		shots_eff=0
	print("\nShot Effectiveness of player ",curr," is ",shots_eff)
	dict1[curr]=shots_eff
	return dict1

def playerContribution(passAcc, duelEff, FK, shot, dict1, minutes):
	keys = passAcc.keys()
	player_cont = 0
	for i in keys:
		try:
			player_cont = (passAcc[i] + duelEff[i] + FK[i] + shot[i])/4
			
			if (minutes[i] == 90):
				player_cont = player_cont * 1.05
			else:
				player_cont = player_cont * (minutes[i]/90)
			print("\nPlayer Contribution of player ",i, " is ",player_cont)
			dict1[i] = player_cont
		except:
			pass
	
	return dict1
	
def playerPerformance(player_contribution, foul_count, own_goal_count, dict1):
    keys = player_contribution.keys()  #get the list of players
    player_performance = 0
    for i in keys:
        player_performance = player_contribution[i]
        if (foul_count[i] != 0):  #fouls
            player_performance -= player_contribution[i] * 0.5 * foul_count[i] / 100

        if (own_goal_count[i] != 0):  #own goals
            player_performance -= player_contribution[i] * 5 * own_goal_count[i] / 100

        print("\nPlayer performance of player ",i," is ",player_performance)
        dict1[i] = player_performance
    
    return dict1    


def playerRating(player_performance, dict1):
    keys = player_performance.keys()
    playerRating = 0
    for i in keys:
        playerRating = (player_performance[i] + 0.5)/2
        print("\nPlayer rating of player ",i," is ",playerRating)
        dict1[i] = playerRating
    return dict1
        

def foul_loss(df1, dict1):
	global playerProfile
	fouls =df1.filter(df1['eventId']==2).orderBy(df1.playerId)
	foul_count=0
	fouls = fouls.select('playerId')
	curr = fouls.collect()[0].playerId
	for row in fouls.collect():
		if row.playerId!=curr:
			playerProfile = playerProfile.withColumn("Fouls", when(playerProfile["Id"]==curr, playerProfile["Fouls"]+foul_count))
			print('\nThe foul count for the player',curr,' is :',foul_count)
			dict1[curr] = foul_count
			curr = row.playerId
			foul_count = 0
		if row.playerId==curr:
			foul_count+=1
	print('\nThe foul count for the player',curr,' is :',foul_count)   
	dict1[curr] = foul_count
	return dict1    

def own_goal(df1, dict1):
	global playerProfile
	goals=df1.orderBy(df1.playerId)
	own_goal = 0
	og_list=[]
	goals = goals.select('playerId', 'tags')
	curr = goals.collect()[0].playerId
	for row in goals.collect():
		if row.playerId!=curr:
			if(own_goal!=0):
				playerProfile = playerProfile.withColumn("Own Goals", when(playerProfile["Id"]==curr, playerProfile["Own Goals"]+1))
			print("\nOwn goal for player ", curr ," is ", own_goal)	
			dict1[curr]=own_goal			
			curr = row.playerId
			own_goal = 0	
		if row.playerId==curr:
			tagslist=row.tags
			for i in tagslist:
				og_list.append(i.id)
			if 102 in og_list:
				own_goal += 1
		og_list=[]	
	print("\nOwn goals for player ", curr ," is ", own_goal)	
	dict1[curr]=own_goal	

	return dict1	

def chemistry(playerChemistry, playerRate, playerTeam ):
	keys = list(playerChemistry.keys())
	for i in keys:
		p1,p2 = i.split("-")
		if playerTeam[int(p1)]==playerTeam[int(p2)]:
			if ((0.5-playerRate[int(p1)] < 0 and 0.5-playerRate[int(p2)] < 0) or (0.5-playerRate[int(p1)] > 0 and 0.5-playerRate[int(p2)] > 0)):
				rate_p1 = playerRate[int(p1)]
				rate_p2 = playerRate[int(p2)]
				new_chem = math.fabs(math.fabs(0.5-rate_p1) - math.fabs(0.5-rate_p2))/2
				playerChemistry[i] += new_chem
				print("\nChemistry for players ", i ," is ", playerChemistry[i]) 
			if ((0.5-playerRate[int(p1)] < 0 and 0.5-playerRate[int(p2)] > 0) or (0.5-playerRate[int(p1)] < 0 and 0.5-playerRate[int(p2)] > 0)):
				new_chem = math.fabs(math.fabs(0.5-playerRate[int(p1)]) - math.fabs(0.5-playerRate[int(p2)]))/2
				playerChemistry[i] -= new_chem 
				print("\nChemistry for players ", i ," is ", playerChemistry[i])
		else:
			if ((0.5-playerRate[int(p1)] < 0 and 0.5-playerRate[int(p2)] < 0) or (0.5-playerRate[int(p1)] > 0 and 0.5-playerRate[int(p2)] > 0)):
				new_chem = math.fabs(math.fabs(0.5-playerRate[int(p1)]) - math.fabs(0.5-playerRate[int(p2)]))/2
				playerChemistry[i] -= new_chem 
				print("\nChemistry for players ", i ," is ", playerChemistry[i])
			if ((0.5-playerRate[int(p1)] < 0 and 0.5-playerRate[int(p2)] > 0) or (0.5-playerRate[int(p1)] < 0 and 0.5-playerRate[int(p2)] > 0)):
				new_chem = math.fabs(math.fabs(0.5-playerRate[int(p1)]) - math.fabs(0.5-playerRate[int(p2)]))/2
				playerChemistry[i] += new_chem
				print("\nChemistry for players ", i ," is ", playerChemistry[i])
	return playerChemistry

def predict(playerChemistry, playerRate, playerTeam):
	teamIDs = list(set(list(playerTeam.values())))
	playersOfTeam1 = []
	playersOfTeam2 = []
	chances = {}
	
	for player in playerTeam:
		if playerTeam[player] == teamIDs[0]:
			playersOfTeam1.append(player)
		if playerTeam[player] == teamIDs[1]:
			playersOfTeam2.append(player)
	
	StrengthsOfTeam1 = []
	StrengthsOfTeam2 = []
	
	for player in playersOfTeam1:
		avgChem = 0
		count = 0
		for chem in playerChemistry:
			p1,p2 = chem.split("-")
			if (int(p1)==player and (int(p2) in playersOfTeam1)):
				avgChem = avgChem + playerChemistry[chem]
				count+=1
			if (int(p2)==player and (int(p1) in playersOfTeam1)):
				avgChem = avgChem + playerChemistry[chem]
				count+=1
		if count!=0:
			avgChem = avgChem / count
		playerStrength = avgChem * playerRate[int(player)]
		StrengthsOfTeam1.append(playerStrength)
	
	for player in playersOfTeam2:
		avgChem = 0
		count = 0
		for chem in playerChemistry:
			p1,p2 = chem.split("-")
			if (int(p1)==player and (int(p2) in playersOfTeam2)):
				avgChem = avgChem + playerChemistry[chem]
				count+=1
			if (int(p2)==player and (int(p1) in playersOfTeam2)):
				avgChem = avgChem + playerChemistry[chem]
				count+=1
		if count!=0:
			avgChem = avgChem / count
		playerStrength = avgChem * playerRate[player]
		StrengthsOfTeam2.append(playerStrength)
	sum1 = 0
	sum2 = 0
	
	for i in StrengthsOfTeam1:
		sum1 = sum1 + i
	for i in StrengthsOfTeam2:
		sum2 = sum2 + i
	
	TotalStrengthTeam1 = sum1/len(StrengthsOfTeam1)
	TotalStrengthTeam2 = sum2/len(StrengthsOfTeam2)
	ChanceOfTeam1 = (0.5 + TotalStrengthTeam1 - ((TotalStrengthTeam1 + TotalStrengthTeam2)/2))*100
	ChanceOfTeam2 = 100 - ChanceOfTeam1
	print("\nChance of Team ",teamIDs[0], " winning: ", ChanceOfTeam1,"%")
	print("\nChance of Team ",teamIDs[1], " winning: ", ChanceOfTeam2,"%")
	chances[teamIDs[0]]=ChanceOfTeam1
	chances[teamIDs[1]]=ChanceOfTeam2
	
	return chances

def readMyStream(rdd):
	global playerProfile
	if not rdd.isEmpty():
		try:
			rows = rdd.flatMap(lambda x:x.split("\n")) #splits rdd based on line
			rows1 = rows.collect() #converts rdd into a list
			df = spark.read.json(sc.parallelize([rows1[0]])) #parallelize converts 'list' type to rdd, and rdd is converted to dataframe
			x = rows1.pop(0) #popping the first row so that df1 does not contain match details, just event details
			df1 = spark.read.json(sc.parallelize(rows1))
			#df.show() 
			#df1.show()
			passAcc = {}
			duelEff = {}
			FK = {}
			shot = {}
			playerCont = {}
			foul = {}
			ownGoal = {}
			playerPerf = {}
			playerRate = {}
			minutes = {}
			playerTeam = {}
			playerChemistry = {}
			players = []
			#df.printSchema()
			
			team_details = df.select('teamsData')
			deets = team_details.collect()[0].teamsData
			teamIDs = list(deets.asDict().keys())
			count = 0
			for i in deets:
				tID = teamIDs[count]
				for j in i.formation.bench:
					passAcc[j.playerId] = 0
					duelEff[j.playerId] = 0
					FK[j.playerId] = 0
					shot[j.playerId] = 0
					playerCont[j.playerId] = 0
					foul[j.playerId] = 0
					ownGoal[j.playerId] = 0
					playerPerf[j.playerId] = 0
					playerRate[j.playerId] = 0.5
					minutes[j.playerId] = 0
					playerTeam[j.playerId] = tID
					#print('\t\t',tID)
					
				for k in i.formation.lineup:
					passAcc[k.playerId] = 0
					duelEff[k.playerId] = 0
					FK[k.playerId] = 0
					shot[k.playerId] = 0
					playerCont[k.playerId] = 0
					foul[k.playerId] = 0
					ownGoal[k.playerId] = 0
					playerPerf[k.playerId] = 0
					playerRate[k.playerId] = 0.5
					minutes[k.playerId] = 90
					playerTeam[k.playerId] = tID
					playerProfile = playerProfile.withColumn("Matches", when(playerProfile["Id"]==k.playerId, playerProfile["Matches"]+1))
					
				for j in i.formation.substitutions:
					minutes[j.playerIn] = 90 - j.minute
					minutes[j.playerOut] = j.minute
					playerProfile = playerProfile.withColumn("Matches", when(playerProfile["Id"]==j.playerIn, playerProfile["Matches"]+1))	
							
				count +=1
				
			players = list(playerTeam.keys())	
			
			for j in players:
				for k in players:
					if(playerTeam[k]==teamIDs[0]):
						if(k!=j):
							if(str(j)+"-"+str(k)) not in playerChemistry.keys():
								playerChemistry[str(k)+"-"+str(j)]=0.5
					else:
						if(k!=j):
							if(str(k)+"-"+str(j)) not in playerChemistry.keys():
								playerChemistry[str(j)+"-"+str(k)]=0.5
								
			print("----------------------------------------------------------METRIC CALCULATIONS BEGIN HERE----------------------------------------------------------")
			passAcc = passAccCalc(df1, passAcc)
			
			duelEff = duelEffCalc(df1, duelEff)
			FK = FKEff(df1, FK)	
			shot = shotEff(df1, shot)	
			foul = foul_loss(df1, foul)
			ownGoal = own_goal(df1, ownGoal)
			playerCont = playerContribution(passAcc, duelEff, FK, shot, playerCont, minutes)
			playerPerf = playerPerformance(playerCont, foul, ownGoal, playerPerf)
			playerRate = playerRating(playerPerf,playerRate)
			playerChemistry = chemistry(playerChemistry, playerRate, playerTeam )
			winningChance = predict(playerChemistry, playerRate, playerTeam)
			print('\n\n\n\n\n')
			#playerProfile.show()
			#df.printSchema()
			#df1.printSchema()
		except:
			pass
		
		
lines.foreachRDD( lambda rdd: readMyStream(rdd) )

ssc.start()
ssc.awaitTermination()

# encoding: utf-8
import sys
sys.path.insert(0, '../libs/deuces/')

from deuces import Card
from deuces import Evaluator
from deuces import Deck
import cards2_judge as cd

def main():
	num_player=3
	card_player=[[['Jd','Ah'],['Js','Jh']],[['2s','9d'],['2h','9s'],['4c','2d']],[['2c','3c'],['Tc','Kc'],['Kc','Qc']]]
	board_player=[['5d','8s','Ac','Jc','4d'],['5d','8s','Ac','Jc','4d'],['5d','8c','Ac','Jc','4d']]
	playermovement=[[1,0,3,6,0],[0,0,3,7,0],[0,0,3,7,0]]
	#playerrank=[[3731,3731,3731],[2487,2487,2487],[4974,4974,4974]]

	playerrank=getRank(num_player,card_player,board_player)
	threat=getPlayerThreat(num_player,playermovement,playerrank)
	print "playerrank is : %s"%playerrank
	print "threat is : %s"%threat



def getRank(num_player,card_player,board_player):
	playerrank=[[]for row in range(num_player)]
	hand=[]*2
	board=[]*5
	for i in range(num_player):
		for t in range(len(card_player[i])):
			hand=[Card.new(card_player[i][t][0]),Card.new(card_player[i][t][1])]
			evaluator=Evaluator()
			board=[Card.new(board_player[i][0]),Card.new(board_player[i][1]),Card.new(board_player[i][2]),Card.new(board_player[i][3]),Card.new(board_player[i][4])]
			rank=evaluator.evaluate(board,hand)
			playerrank[i].append(rank)
			print hand,rank,playerrank[i]
	print playerrank
	return playerrank

def getPlayerThreat(num_player,playermovement,playerrank):
	threat=[None]*num_player
	for i in range(num_player):
		ave_move = float(playermovement[i][0]+2*playermovement[i][1]+3*playermovement[i][2]+4*playermovement[i][3]+5*playermovement[i][4])\
			/(playermovement[i][0]+playermovement[i][1]+playermovement[i][2]+playermovement[i][3]+playermovement[i][4])
		a=7461*len(playerrank[i])-sum(playerrank[i])
		b=len(playerrank[i])
		print "a,b is : %s"% a,b,ave_move
		ave_rank = float(a)/b
		temp=float(ave_rank/ave_move)
		if  temp<550:
			threat[i]=1
		elif temp >1850:
			threat[i]=3
		else :
			threat[i]=2
		print "ave_rank/ sum_move is :%s"%temp
	print "threat is %s"%threat

	return threat
main()
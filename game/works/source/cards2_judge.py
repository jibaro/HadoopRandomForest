import cards2_careful
import cards2_strong
import cards2_normal

#playermovement[num_player][num_fold,num_check,num_call,num_raise,num_all_in]
#playerrank[num_player][rank1...rankn]
#card_player[num_player][7]




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
		a=7461*num_player-sum(playerrank[i])
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


def makeDecisionBlindFinal(card,cardround,oppobehave,oppobehavenum,num_player,playermovement,card_player):
	playerrank=getRank(num_player,card_player,board_player)
	threat=getPlayerThreat(num_player,playermovement,playerrank)
	if max(threat)==3:
		card2s_careful.makeDecisionBlind(card,cardround,oppobehave,oppobehavenum,num_player)
	elif max(threat)==2:
		card2s_normal.makeDecisionBlind(card,cardround,oppobehave,oppobehavenum,num_player)
	else:
		card2s_strong.makeDecisionBlind(card,cardround,oppobehave,oppobehavenum,num_player)


def makeDecisionFlopFinal(card,cardround,percentage,oppobehave,oppobehavenum,num_player,playermovement,playerrank):
	playerrank=getRank(num_player,card_player,board_player)
	threat=getPlayerThreat(num_player,playermovement,playerrank)
	if max(threat)==3:
		card2s_careful.makeDecisionFlop(card,cardround,oppobehave,oppobehavenum,num_player)
	elif max(threat)==2:
		card2s_normal.makeDecisionFlop(card,cardround,oppobehave,oppobehavenum,num_player)
	else:
		card2s_strong.makeDecisionFlop(card,cardround,oppobehave,oppobehavenum,num_player)


def makeDecisionTurnFinal(card,cardround,percentage,oppobehave,oppobehavenum,num_player,playermovement,playerrank):
	playerrank=getRank(num_player,card_player,board_player)
	threat=getPlayerThreat(num_player,playermovement,playerrank)
	if max(threat)==3:
		card2s_careful.makeDecisionTurn(card,cardround,oppobehave,oppobehavenum,num_player)
	elif max(threat)==2:
		card2s_normal.makeDecisionTurn(card,cardround,oppobehave,oppobehavenum,num_player)
	else:
		card2s_strong.makeDecisionTurn(card,cardround,oppobehave,oppobehavenum,num_player)

def makeDecisionRiverFinal(card,cardround,oppobehave,oppobehavenum,num_player,playermovement,playerrank):
	playerrank=getRank(num_player,card_player,board_player)
	threat=getPlayerThreat(num_player,playermovement,playerrank)

	if max(threat)==3:
		card2s_careful.makeDecisionRiver(card,cardround,oppobehave,oppobehavenum,num_player,threat)
	elif max(threat)==2:
		card2s_normal.makeDecisionRiver(card,cardround,oppobehave,oppobehavenum,num_player,threat)
	else:
		card2s_strong.makeDecisionRiver(card,cardround,oppobehave,oppobehavenum,num_player,threat)






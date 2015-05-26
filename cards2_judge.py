import cards2_careful
import cards2_strong
import cards2_normal

#playermovement[num_player][num_fold,num_check,num_call,num_raise,num_all_in]
#playerrank[num_player][rank1...rankn]
#card_player[num_player][7]
def getRank(num_player,card_player):
	try:
		for i in range(num_player):
			hand[i]=[Card.new(card_player[i][0]),Card.new(card_player[i][1])]
			evaluator[i]=Evaluator()
			board=[Card.new(card_player[i][2]),Card.new(card_player[i][3]),Card.new(card_player[i][4]),Card.new(card_player[i][5]),Card.new(card_player[i][6])]
			rank[i]=evaluator.evaluate(board,hand[i])
			playerrank[i].append(rank[i])
	except:
		break
	return playerrank

def getPlayerThreat(num_player,playermovement,playerrank):
	for i in range(num_player):
		sum_move = float((playermovement[i][num_fold]+1865*playermovement[i][num_check]+3730*playermovement[i][num_call]+5596*playermovement[i][num_raise]+7462*playermovement[i][num_all_in])\
		/(playermovement[i][num_fold]+playermovement[i][num_check]+playermovement[i][num_call]+playermovement[i][num_raise]+playermovement[i][num_all_in]))
		ave_rank = float(sum(7462-playerrank[i])/len(playerrank[i]))
		if ave_rank/sum_move >1.3:
			threat[i]=3
		elif ave_rank/sum_move < 0.7:
			threat[i]=1
		else :
			threat[i]=2
	return threat


def makeDecisionBlindFinal(card,cardround,oppobehave,oppobehavenum,num_player,playermovement,card_player):
	playerrank=getRank(num_player,card_player)
	threat=getPlayerThreat(num_player,playermovement,playerrank)
	if max(threat)==3:
		card2s_careful.makeDecisionBlind(card,cardround,oppobehave,oppobehavenum,num_player)
	elif max(threat)==2:
		card2s_normal.makeDecisionBlind(card,cardround,oppobehave,oppobehavenum,num_player)
	else:
		card2s_strong.makeDecisionBlind(card,cardround,oppobehave,oppobehavenum,num_player)


def makeDecisionFlopFinal(card,cardround,percentage,oppobehave,oppobehavenum,num_player,playermovement,playerrank):
	playerrank=getRank(num_player,card_player)
	threat=getPlayerThreat(num_player,playermovement,playerrank)
	if max(threat)==3:
		card2s_careful.makeDecisionFlop(card,cardround,oppobehave,oppobehavenum,num_player)
	elif max(threat)==2:
		card2s_normal.makeDecisionFlop(card,cardround,oppobehave,oppobehavenum,num_player)
	else:
		card2s_strong.makeDecisionFlop(card,cardround,oppobehave,oppobehavenum,num_player)


def makeDecisionTurnFinal(card,cardround,percentage,oppobehave,oppobehavenum,num_player,playermovement,playerrank):
	playerrank=getRank(num_player,card_player)
	threat=getPlayerThreat(num_player,playermovement,playerrank)
	if max(threat)==3:
		card2s_careful.makeDecisionTurn(card,cardround,oppobehave,oppobehavenum,num_player)
	elif max(threat)==2:
		card2s_normal.makeDecisionTurn(card,cardround,oppobehave,oppobehavenum,num_player)
	else:
		card2s_strong.makeDecisionTurn(card,cardround,oppobehave,oppobehavenum,num_player)

def makeDecisionRiverFinal(card,cardround,oppobehave,oppobehavenum,num_player,playermovement,playerrank):
	playerrank=getRank(num_player,card_player)
	threat=getPlayerThreat(num_player,playermovement,playerrank)

	if max(threat)==3:
		card2s_careful.makeDecisionRiver(card,cardround,oppobehave,oppobehavenum,num_player,threat)
	elif max(threat)==2:
		card2s_normal.makeDecisionRiver(card,cardround,oppobehave,oppobehavenum,num_player,threat)
	else:
		card2s_strong.makeDecisionRiver(card,cardround,oppobehave,oppobehavenum,num_player,threat)






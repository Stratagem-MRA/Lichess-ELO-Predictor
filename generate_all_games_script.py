import pandas as pd
import os
import cairosvg
import chess.pgn
import chess.svg
import io
from multiprocessing import Pool

def update_chess_dtypes(df):

    #converts each DataFrame column to the correct dtype

    df['Event'] = df['Event'].astype("string")
    df['ID'] = df['ID'].astype("string")
    df['Result']  = df['Result'].astype("Int8")
    df['WhiteElo'] = df['WhiteElo'].astype("UInt16")
    df['BlackElo'] = df['BlackElo'].astype("UInt16")
    df['TimeControl'] = df['TimeControl'].astype("string")
    df['Termination'] = df['Termination'].astype("string")
    df['Moves'] = df['Moves'].astype("string")
    df['NumMoves'] = df['NumMoves'].astype("UInt16") 
    return df

def board_png_from_row(row, num_moves):

    #given a row entry from df and the move number
    #saves a png (size: svg_size x svg_size) of the position to ./Data/PNGImages

    if num_moves > row['NumMoves']:
        raise(Exception(f"num_moves exceeds maximum moves:{row['NumMoves']}"))
    png_path = f"Data/PGNImages/{row['ID']}_{row['WhiteElo']}_{row['BlackElo']}_{num_moves}.png"
    if not os.path.isfile(png_path):
        moves = io.StringIO(row['Moves'])
        game = chess.pgn.read_game(moves)
        board = game.board()
        iter = game.mainline_moves().__iter__()
        #+1 prevents us from generating default board
        for _ in range(num_moves+1):
            board.push(next(iter))
        cairosvg.svg2png(chess.svg.board(board, coordinates=False, size=8*10),write_to=png_path)

def generate_full_game(row):
    for i in range(row['NumMoves']):
        board_png_from_row(row, i)

def generate_all_games(iterrow_tuple):
	idx = iterrow_tuple[0]
	#print(f'row {idx}',end='\r')
	row = iterrow_tuple[1]
	generate_full_game(row)
    
if __name__ == '__main__':
	if os.path.isfile('Data/chessgames-2013-01.csv'):
		df = pd.read_csv('Data/chessgames-2013-01.csv',index_col=0)
		df = update_chess_dtypes(df)
		print(f'loaded Data/chessgames-2013-01.csv into df')
	with Pool(6) as p:
		p.map(generate_all_games, df.iterrows())
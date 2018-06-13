from flask import Flask, render_template
import json, sqlalchemy

app = Flask(__name__)

engine = sqlalchemy.create_engine('mysql+pymysql://austinboling57@gmail.com:6Austin14@127.0.0.1/DataBase?charset=utf8mb4') # replace <> parts in the string

@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!\n\nAppend /recommendation/<userid> to the current url\n\nSome availble userids: 76561198249026172, 76561198082481473, 76561198040992485, 76561197960464402"


@app.route('/recommendation/<userid>')
def recommendation(userid):
	result = engine.execute('''
		SELECT g0,g1,g2,g3,g4,g5,g6,g7,g8,g9 FROM tbl_recommended_games WHERE user_id=%s;
		''' % userid).first()


	lst_recommended_games = []
	for app_id in list(result):
		app_data = engine.execute('''
						SELECT name,initial_price,header_image FROM tbl_steam_app WHERE steam_appid = %s;
					''' % app_id).first()
		if app_data != None:
			lst_recommended_games.append(app_data)


	return render_template( 'recommendation.html',
							userid = userid,
							lst_recommended_games = lst_recommended_games)


if __name__ == '__main__':
	app.run(debug=True)




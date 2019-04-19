from flask import Flask, jsonify, abort, make_response, request
from flask_sqlalchemy import SQLAlchemy 
from flask_marshmallow import Marshmallow 
import os
import sqlite3
from datetime import timedelta, date
from sqlalchemy import and_, or_, func

conn = sqlite3.connect("db.apitruckpad")
c = conn.cursor()

config_db = open('config_db.sql', 'r').read()
create_estado = open('create_estado.sql', 'r').read()
create_municipio = open('create_municipio.sql', 'r').read()
create_caminhao = open('create_caminhao.sql', 'r').read()
create_motorista = open('create_motorista.sql', 'r').read()
create_frete = open('create_frete.sql', 'r').read()
c.execute(config_db)
c.execute(create_estado)
c.execute(create_municipio)
c.execute(create_caminhao)
c.execute(create_motorista)
c.execute(create_frete)

ins_municipio = open('insert_municipio.sql', 'r').read()  
ins_estado = open('insert_estado.sql', 'r').read()
#c.execute(ins_estado)
#c.execute(ins_municipio)

a = c.fetchall()
conn.commit()

app = Flask('app')

basedir = os.path.abspath(os.path.dirname(__file__))
# Database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'db.apitruckpad')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# Init db
db = SQLAlchemy(app)
# Init ma
ma = Marshmallow(app)

class Estado(db.Model):
  codigo_uf = db.Column(db.Integer, primary_key=True)
  uf = db.Column(db.String(2), unique=False)
  nome = db.Column(db.String(100), unique=False)
     
  def __init__(codigo_uf, uf, nome):
    self.codigo_uf = codigo_uf
    self.uf = uf
    self.nome = nome
    
# Estado Schema
class EstadoSchema(ma.Schema):
  class Meta:
    fields = ('codigo_uf', 'uf', 'nome')

# Init schema
estado_schema = EstadoSchema(strict=True)
estados_schema = EstadoSchema(many=True, strict=True)

class Municipio(db.Model):
  codigo_ibge = db.Column(db.Integer, primary_key=True)
  nome = db.Column(db.String(200), unique=False)
  latitude = db.Column(db.Float, unique=False)
  longitude = db.Column(db.Float, unique=False)
  capital = db.Column(db.Integer, unique=False)
  codigo_uf = db.Column(db.Integer, db.ForeignKey('estado.codigo_uf'))
     
  def __init__(codigo_ibge, nome, latitude, longitude, capital, codigo_uf):
    self.codigo_ibge = codigo_ibge
    self.nome = nome
    self.latitude = latitude
    self.longitude = longitude
    self.capital = capital
    self.codigo_uf = codigo_uf
    
# Estado Schema
class MunicipioSchema(ma.Schema):
  class Meta:
    fields = ('codigo_ibge', 'nome', 'latitude', 'longitude', 'capital', 'codigo_uf')

# Init schema
municipio_schema = MunicipioSchema(strict=True)
municipios_schema = MunicipioSchema(many=True, strict=True)

class Caminhao(db.Model):
  id = db.Column(db.Integer, primary_key=True)
  tipo = db.Column(db.String(100), unique=True)
  
  def __init__(self, tipo):
    self.tipo = tipo
    
# Caminhao Schema
class CaminhaoSchema(ma.Schema):
  class Meta:
    fields = ('id', 'tipo')

# Init schema
caminhao_schema = CaminhaoSchema(strict=True)
caminhoes_schema = CaminhaoSchema(many=True, strict=True)

class Motorista(db.Model):
  id = db.Column(db.Integer, primary_key=True)
  nome = db.Column(db.String(100), unique=False)
  idade = db.Column(db.Integer, unique=False)
  sexo = db.Column(db.String(1), unique=False)
  cnh = db.Column(db.String(1), unique=False)
      
  def __init__(self, nome, idade, sexo, cnh):
    self.nome = nome
    self.idade = idade
    self.sexo = sexo
    self.cnh = cnh
        
# Motorista Schema
class MotoristaSchema(ma.Schema):
  class Meta:
    fields = ('id', 'nome', 'idade', 'sexo', 'cnh')

# Init schema
motorista_schema = MotoristaSchema(strict=True)
motoristas_schema = MotoristaSchema(many=True, strict=True)

class Frete(db.Model):
  id = db.Column(db.Integer, primary_key=True)
  carregado = db.Column(db.Integer, unique=False)
  caminhao_proprio = db.Column(db.Integer, unique=False)
  caminhao_id = db.Column(db.Integer, db.ForeignKey('caminhao.id'))
  motorista_id = db.Column(db.Integer, db.ForeignKey('motorista.id'))
  origem_id = db.Column(db.Integer, db.ForeignKey('municipio.codigo_ibge'))
  destino_id = db.Column(db.Integer, db.ForeignKey('municipio.codigo_ibge'))
  data = db.Column(db.String(10), unique=False)
     
  def __init__(self, carregado, caminhao_proprio, caminhao_id, motorista_id, origem_id, destino_id, data):
    self.carregado = carregado
    self.caminhao_proprio = caminhao_proprio
    self.caminhao_id = caminhao_id
    self.motorista_id = motorista_id
    self.origem_id = origem_id
    self.destino_id = destino_id
    self.data = data
    
# Frete Schema
class FreteSchema(ma.Schema):
  class Meta:
    fields = ('id', 'carregado', 'caminhao_proprio', 'caminhao_id', 'motorista_id', 'origem_id', 'destino_id', 'data')

# Init schema
frete_schema = FreteSchema(strict=True)
fretes_schema = FreteSchema(many=True, strict=True)

class Frete_Grupo:
  def __init__(self, tipo_caminhao, origem, destino):
    self.tipo_caminhao = tipo_caminhao
    self.origem = origem
    self.destino = destino    
    
# Frete_Grupo Schema
class Frete_GrupoSchema(ma.Schema):
  class Meta:
    fields = ('tipo_caminhao','origem','destino')

# Init schema
frete_grupo_schema = Frete_GrupoSchema(strict=True)
frete_grupos_schema = Frete_GrupoSchema(many=True, strict=True)

@app.route('/api/v1.0/caminhoes', methods=['GET'])
def get_caminhoes():
  all_caminhoes = Caminhao.query.all()
  result = caminhoes_schema.dump(all_caminhoes)
  return jsonify(result.data)

@app.route('/api/v1.0/caminhoes/<int:id>', methods=['GET'])
def get_caminhao(id):
  caminhao = [caminhao for caminhao in caminhoes if caminhao['id'] == id]
  if len(caminhao) == 0:
    abort(404)
  caminhao = Caminhao.query.get(id)
  return caminhao_schema.jsonify(caminhao)

@app.route('/api/v1.0/caminhoes', methods=['POST'])
def adicionar_caminhao():
  if not request.json or not 'tipo' in request.json:
        abort(400)

  tipo = request.json['tipo']
  
  novo_caminhao = Caminhao(tipo)
  db.session.add(novo_caminhao)
  db.session.commit()

  return caminhao_schema.jsonify(novo_caminhao)

@app.route('/api/v1.0/caminhoes/<int:id>', methods=['PUT'])
def update_caminhao(id):
  if id == None:
    abort(404)
  if not request.json:
    abort(400)

  tipo = request.json['tipo']  

  caminhao = Caminhao.query.get(id)
  caminhao.tipo = tipo
  db.session.commit()

  return caminhao_schema.jsonify(caminhao)
  
@app.route('/api/v1.0/motoristas', methods=['GET'])
def get_motoristas():
  all_motoristas = Motorista.query.all()
  result = motoristas_schema.dump(all_motoristas)
  return jsonify(result.data)

@app.route('/api/v1.0/motoristas/<int:id>', methods=['GET'])
def get_motorista(id):
    motorista = [motorista for motorista in motoristas if motorista['id'] == id]
    if motorista == None:
        abort(404)
    
    motorista = Motorista.query.get(id)
    return motorista_schema.jsonify(motorista)

@app.route('/api/v1.0/motoristas', methods=['POST'])
def adicionar_motorista():
  nome = request.json['nome']
  idade = request.json['idade']
  sexo = request.json['sexo']
  cnh = request.json['cnh']
    
  novo_motorista = Motorista(nome, idade, sexo, cnh)
  db.session.add(novo_motorista)
  db.session.commit()

  return motorista_schema.jsonify(novo_motorista)

@app.route('/api/v1.0/motoristas/<int:id>', methods=['PUT'])
def update_motorista(id):
  if id == None:
    abort(404)
  if not request.json:
    abort(400)

  nome = request.json['nome']  
  idade = request.json['idade']  
  sexo = request.json['sexo']  
  cnh = request.json['cnh']  
  
  motorista = Motorista.query.get(id)
  motorista.nome = nome
  motorista.idade = idade
  motorista.sexo = sexo
  motorista.cnh = cnh
  db.session.commit()

  return motorista_schema.jsonify(motorista)

@app.route('/api/v1.0/fretes', methods=['GET'])
def get_fretes():
  all_fretes = Frete.query.all()
  result = fretes_schema.dump(all_fretes)
  return jsonify(result.data)

@app.route('/api/v1.0/fretes/<int:id>', methods=['GET'])
def get_frete(id):
  frete = [frete for frete in fretes if frete['id'] == id]
  if len(frete) == 0:
    abort(404)
  frete = Frete.query.get(id)
  return frete_schema.jsonify(frete)

@app.route('/api/v1.0/fretes', methods=['POST'])
def adicionar_frete():
  if not request.json or not 'carregado' in request.json:
        abort(400)

  carregado = request.json['carregado']
  caminhao_proprio = request.json['caminhao_proprio']
  caminhao_id = request.json['caminhao_id']
  motorista_id = request.json['motorista_id']
  origem_id = request.json['origem_id']
  destino_id = request.json['destino_id']
  data = request.json['data']
    
  novo_frete = Frete(carregado, caminhao_proprio, caminhao_id, motorista_id, origem_id, destino_id, data)
  db.session.add(novo_frete)
  db.session.commit()

  return frete_schema.jsonify(novo_frete)

@app.route('/api/v1.0/frete/<int:id>', methods=['PUT'])
def update_frete(id):
  if id == None:
    abort(404)
  if not request.json:
    abort(400)

  carregado = request.json['carregado']
  caminhao_proprio = request.json['caminhao_proprio']
  caminhao_id = request.json['caminhao_id']
  motorista_id = request.json['motorista_id']
  origem_id = request.json['origem_id']
  destino_id = request.json['destino_id']
  data = request.json['data']

  frete = Frete.query.get(id)
  frete.carregado = carregado
  frete.caminhao_proprio = caminhao_proprio
  frete.caminhao_id = caminhao_id
  frete.motorista_id = motorista_id
  frete.origem_id = origem_id
  frete.destino_id = destino_id
  frete.data = data
  db.session.commit()

  return frete_schema.jsonify(frete)

@app.route('/api/v1.0/fretes/descarregados', methods=['GET'])
def get_descarregados():
  descarregados = Frete.query.filter(Frete.carregado == 0)
  return fretes_schema.jsonify(descarregados)

@app.route('/api/v1.0/fretes/qtdcarregadosdia', methods=['GET'])
def get_qtd_carregados_dia():
  data_atual = date.today()
  carregados_dia = Frete.query.filter(and_(Frete.carregado == 1, Frete.data == data_atual)).count()
  return jsonify(carregados_dia)

@app.route('/api/v1.0/fretes/qtdcarregadossemana', methods=['GET'])
def get_qtd_carregados_semana():
  data_atual = date.today()
  data_semana_passada = data_atual - timedelta(7)
  print(data_atual)
  print(data_semana_passada)
  carregados_semana = Frete.query.filter(and_(Frete.carregado == 1, Frete.data.between(data_semana_passada, data_atual))).count()
  return jsonify(carregados_semana)

@app.route('/api/v1.0/fretes/qtdcarregadosmes', methods=['GET'])
def get_qtd_carregados_mes():
  data_atual = date.today()
  data_mes_passado = data_atual - timedelta(30)
  print(data_atual)
  print(data_mes_passado)
  carregados_mes = Frete.query.filter(and_(Frete.carregado == 1, Frete.data.between(data_mes_passado, data_atual))).count()
  return jsonify(carregados_mes)

@app.route('/api/v1.0/fretes/qtdcaminhaoproprio', methods=['GET'])
def get_qtd_caminhao_proprio():
  caminhao_proprio = Frete.query.filter(Frete.caminhao_proprio == 1).count()
  return jsonify(caminhao_proprio)

@app.route('/api/v1.0/fretes/agrupados', methods=['GET'])
def get_agrupados():
  agrupados = Frete.query.all()
  resultado = []
  for frete in agrupados:
    if frete.caminhao_id == 1:
      novo_agrupamento = Frete_Grupo(frete.caminhao_id, frete.origem_id, frete.destino_id)
      resultado.append(novo_agrupamento)
    
  for frete in agrupados:
    if frete.caminhao_id == 2:
      novo_agrupamento = Frete_Grupo(frete.caminhao_id, frete.origem_id, frete.destino_id)
      resultado.append(novo_agrupamento)

  for frete in agrupados:
    if frete.caminhao_id == 3:
      novo_agrupamento = Frete_Grupo(frete.caminhao_id, frete.origem_id, frete.destino_id)
      resultado.append(novo_agrupamento)

  for frete in agrupados:
    if frete.caminhao_id == 4:
      novo_agrupamento = Frete_Grupo(frete.caminhao_id, frete.origem_id, frete.destino_id)
      resultado.append(novo_agrupamento)    
    
  for frete in agrupados:
    if frete.caminhao_id == 5:
      novo_agrupamento = Frete_Grupo(frete.caminhao_id, frete.origem_id, frete.destino_id)
      resultado.append(novo_agrupamento)

  return frete_grupos_schema.jsonify(resultado)

@app.route('/api/v1.0/estados', methods=['GET'])
def get_estados():
  all_estados = Estado.query.all()
  result = estados_schema.dump(all_estados)
  return jsonify(result.data)

@app.route('/api/v1.0/municipios', methods=['GET'])
def get_municipios():
  all_municipios = Municipio.query.all()
  result = municipios_schema.dump(all_municipios)
  return jsonify(result.data)  

@app.route('/')
def hello_world():
  return 'TruckPad Challenge!'

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

app = Flask(__name__)

#app.run(host='0.0.0.0', port=8080)

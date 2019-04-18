CREATE TABLE IF NOT EXISTS municipio(
    codigo_ibge INTEGER PRIMARY KEY,
    nome TEXT,
    latitude REAL,
    longitude REAL,
    capital INTEGER,
    codigo_uf INTEGER,
    FOREIGN KEY (codigo_uf) REFERENCES estado(codigo_uf));
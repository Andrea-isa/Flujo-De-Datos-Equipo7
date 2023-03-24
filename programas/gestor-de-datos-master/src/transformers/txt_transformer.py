##!/usr/bin/env python
# -- coding: utf-8 --
#-------------------------------------------------------------------------
# Archivo: txt_transformer.py
# Autor(es):Equipo 7
# Version: 1.0.0 Marzo 2023
# Descripción:
#
#   Este archivo define un procesador de datos que se encarga de transformar
#   y formatear el contenido de un archivo TXT
#-------------------------------------------------------------------------
from src.extractors.txt_extractor import TXTExtractor
from os.path import join
import luigi, os, csv, json, re

class TXTTransformer(luigi.Task):

    def requires(self):
        return TXTExtractor()
    
    def run(self):
        result = []
        lineas=[]
        parametros=[]
        for file in self.input():
            with file.open() as txt_file:
                next(txt_file)
                cabecera = txt_file.read();
                lineas.append(cabecera.split(';'))
                for i in lineas:
                    parametros.append(lineas[i].split(','))
                    for j in parametros:    
                        entry = parametros[j]

                    result.append(
                        {
                            "description": entry["productdesc"],
                            "quantity": entry["qty"],
                            "price": entry["rawprice"],
                            "total": float(entry["qty"]) * float(entry["rawprice"]),
                            "invoice": entry["inv"],
                            "provider": entry["provider"],
                            "country": entry["countryname"]
                        }
                    )
        with self.output().open('w') as out:
            out.write(json.dumps(result, indent=4))

    def output(self):
        project_dir = os.path.dirname(os.path.abspath("loader.py"))
        result_dir = join(project_dir, "result")
        return luigi.LocalTarget(join(result_dir, "txt.json"))

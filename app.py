#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
TheMoviePredictor script
Author: Arnaud de Mouhy <arnaud@admds.net>
"""

import mysql.connector
import sys
import argparse
import csv

def connectToDatabase():
    return mysql.connector.connect(user='predictor', password='predictor',
                              host='127.0.0.1',
                              database='predictor')

def disconnectDatabase(cnx):
    cnx.close()

def createCursor(cnx):
    return cnx.cursor(dictionary=True)

def closeCursor(cursor):    
    cursor.close()

def findQuery(table, id):
    return ("SELECT * FROM {} WHERE id = {}".format(table, id))

def findAllQuery(table):
    return ("SELECT * FROM {}".format(table))

def insertMovieQuery(table, title, duration, original_title, release_date, rating):
    return("INSERT INTO {} ('{}', '{}', '{}', '{}', '{}')".format(table, title, duration, original_title, release_date, rating))

def insertPeopleQuery(table, firstname, lastname):
    return("INSERT INTO {} (firstname, lastname) VALUES ('{}', '{}')".format(table, firstname, lastname))

def insertPeople(table, firstname, lastname):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    cursor.execute(insertPeopleQuery(table, firstname, lastname))
    cnx.commit()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    

def insertMovie(table, title, duration, original_title, release_date, rating):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    cursor.execute(insertMovieQuery(table, title, duration, original_title, release_date, rating))
    cnx.commit()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    

def find(table, id):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    query = findQuery(table, id)
    cursor.execute(query)
    results = cursor.fetchall()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results

def findAll(table):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    cursor.execute(findAllQuery(table))
    results = cursor.fetchall()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results

def printPerson(person):
    print("#{}: {} {}".format(person['id'], person['firstname'], person['lastname']))

def printMovie(movie):
    print("#{}: {} released on {}".format(movie['id'], movie['title'], movie['release_date']))

parser = argparse.ArgumentParser(description='Process MoviePredictor data')

parser.add_argument('context', choices=['people', 'movies', 'insert'], help='Le contexte dans lequel nous allons travailler')

action_subparser = parser.add_subparsers(title='action', dest='action')

list_parser = action_subparser.add_parser('list', help='Liste les entitees du contexte')
list_parser.add_argument('--export' , help='Chemin du fichier exporte')

find_parser = action_subparser.add_parser('find', help='Trouve une entitee selon un parametre')
find_parser.add_argument('id' , help='Identifant a rechercher')

parser_insert = action_subparser.add_parser('insert', help='Inserer valeurs')

#Insert People

parser_insert.add_argument('--firstname', metavar='firstname', help='Insérer le prénom')
parser_insert.add_argument('--lastname', metavar='lastname', help='Insérer le nom')

#Insert Film

parser_insert.add_argument('--title', metavar='title', help='Titre')
parser_insert.add_argument('--original_title', metavar='original_title', help='Titre original')
parser_insert.add_argument('--rating', help='Age recommandé pour voir le film')
parser_insert.add_argument('--release_date', help='Date de sortie du film')

args = parser.parse_args()

if args.context == "people":
    if args.action == "list":
        people = findAll("people")
        if args.export:
            with open(args.export, 'w', encoding='utf-8', newline='\n') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(people[0].keys())
                for person in people:
                    writer.writerow(person.values())
        else:
            for person in people:
                printPerson(person)
    if args.action == "find":
        peopleId = args.id
        people = find("people", peopleId)
        for person in people:
            printPerson(person)
    if args.action == "insert":
        insertPeople('people', args.firstname, args.lastname)


if args.context == "movies":
    if args.action == "list":  
        movies = findAll("movies")
        for movie in movies:
            printMovie(movie)
    if args.action == "find":  
        movieId = args.id
        movies = find("movies", movieId)
        for movie in movies:
            printMovie(movie)
    if args.action == "insert":
        insertMovie('movies', args.title, args.duration, args.original_title, args.release_date, args.rating)
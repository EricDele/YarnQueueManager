#!/usr/bin/python
# coding: utf-8

# ===================================================================#
# -------------------------------------------------------------------#
#                         YarnQueueManager                           #
# -------------------------------------------------------------------#
# *******************************************************************#
#                   Eric Deleforterie - 2016/11/21                   #
# -------------------------------------------------------------------#
#                          Notes/Commentaires                        #
#                                                                    #
# -------------------------------------------------------------------#
#                             HISTORIQUE                             #
#    V0.0.1    Eric Deleforterie - 2016/11/21                        #
#              Création des premières fonctionnalités                #
# ===================================================================#


# --------------------------------------------#
#           Importation des packages          #
# --------------------------------------------#
from __future__ import print_function
import sys
import csv
import json
import pprint
import argparse
import xlsxwriter
from openpyxl import load_workbook
from lxml import etree
from collections import defaultdict

# --------------------------------------------#
#           Declaration des variables         #
# --------------------------------------------#
global vg_fileName
global vg_arguments
global vg_delimiter
global vg_xmlcontent
global vg_config_root
global vg_xml2xls
global vg_xlsConfig
global vg_queues
vg_queues = defaultdict(dict)

DEFAULT = '\033[39m'
RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
BACK_BLUE = '\033[44m'
BACK_DEFAULT = '\033[49m'

# --------------------------------------------#
#                 Classe Queue                #
# --------------------------------------------#

class Queues():
    """Object for Queues"""

    def __init__(self):
        self.queues = defaultdict(dict)

    def addQueueValue(self, configXLS, arborescence, queueName, propertyName, value):
        try:
            self.queues[arborescence + "." + queueName][propertyName] = value
        except Exception as e:
            raise e
        # if propertyName in configXLS:
        #     # test si la colonne doit exister dans le fichier xls
        #     if configXLS[propertyName]['column']:
        #         self.queues[queueName][configXLS[propertyName]['column']] = value
        #     else:
        #         print(YELLOW + "WARNING : La propriété " + propertyName + " n'a pas de colonne de renseignée dans le fichier de configuration" + DEFAULT)
        # else:
        #     # Erreur de configuration
        #     print(RED + "ERROR : La propriété " + propertyName + " n'est pas présente dans le fichier de configuration" + DEFAULT)

    def showQueue(self):
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(dict(self.queues))

    @staticmethod
    def lettreVersNum(lettre):
        return ord(lettre)

    @staticmethod
    def lettreVersCol(lettre):
        if(len(lettre) == 1):
            return (ord(lettre.upper()) - ord('A')) + 1

    @staticmethod
    def colVersLettre(column):
        if(type(column) == int):
            return chr(column + ord('A') - 1) 

    @staticmethod
    def numVersLettre(chiffre):
        if (chiffre >= ord('a') and chiffre <= ord('z')) or (chiffre >= ord('A') and chiffre <= ord('Z')):
            return chr(chiffre)

    def readXlsFile(self, fileXLS, configXLS):
        wb = load_workbook(fileXLS, data_only=True)
        ws = wb.get_sheet_by_name(configXLS['sheet-name'])
        row = int(configXLS['cellule-origine']['row'])
        col = int(configXLS['cellule-origine']['col'])
        rowMax = int(configXLS['row-max'])
        arborescence = []
        # contrôle de cohérence sur les titres de colonnes
        for column in configXLS['topologie']:
            if(ws[column + configXLS['row-titles']].value != configXLS['topologie'][column]['columnTitle']):
                # Erreur de contrôle sur le titre de colonne
                print(RED + "ERROR : analyse des colonnes du fichier excel incohérent dans la cellule " + column + configXLS['row-titles'] +
                      " trouvé : " + ws[column + configXLS['row-titles']].value + " au lieu de : " + configXLS['topologie'][column]['columnTitle'] + DEFAULT)
                return False
        # parsing du fichier
        arboActuelle = ""
        while row != rowMax:
            if(ws.cell(row=row, column=col).value is None and ws.cell(row=row, column=col + 1).value is None):
                # Ligne vide, remise à blanc de l'arborescence
                arboActuelle = ""
            elif(ws.cell(row=row, column=col).value is not None and ws.cell(row=row, column=int(configXLS['queues-name-column'])).value is None):
                # Nouvelle arborescence
                arboActuelle = ws.cell(row=row, column=col).value
                print(BACK_BLUE + CYAN + "Nouvelle ARBO : " + arboActuelle + DEFAULT + BACK_DEFAULT)
            elif(ws.cell(row=row, column=int(configXLS['queues-name-column'])).value is not None):
                # Queue trouvée
                queueName = str(ws.cell(row=row, column=int(configXLS['queues-name-column'])).value)
                #print('Queue trouvée :' + queueName)
                # On itère sur la ligne avec les colonnes configurées
                for column in sorted(configXLS['topologie']):
                    # memorisation de la valeur de la cellule
                    cellValue = str(ws.cell(row=row, column=self.lettreVersCol(column)).value)
                    # Si on est pas sur la queue et sur une colonne après les queues ( hors arborescence )
                    if(column.upper() != self.colVersLettre(int(configXLS['queues-name-column'])) and column.upper() > self.colVersLettre(int(configXLS['queues-name-column']))):
                        # on a joute la donnée si elle est présente, sinon on ajoute la valeur par défaut
                        if(cellValue != 'None'):
                            self.addQueueValue(configXLS, arboActuelle, queueName, configXLS['topologie'][column]['property'], cellValue)
                            sys.stdout.write(GREEN + configXLS['topologie'][column]['property'] + ": " + cellValue + ", " + DEFAULT)
                        # valeur par defaut
                        elif(configXLS['topologie'][column]['default'] != ""):
                            self.addQueueValue(configXLS, arboActuelle, queueName, configXLS['topologie'][column]['property'], configXLS['topologie'][column]['default'])
                            sys.stdout.write(YELLOW + configXLS['topologie'][column]['property'] + ": " + cellValue + ", " + DEFAULT)
                        # pas de valeur par defaut, on ajoute pas
                        else:
                            sys.stdout.write(RED + configXLS['topologie'][column]['property'] + ": " + cellValue + ", " + DEFAULT)
                    # On est sur la queue
                    elif(column.upper() == self.colVersLettre(int(configXLS['queues-name-column']))):
                        if(arboActuelle != ""):
                            sys.stdout.write("Arbo : " + CYAN + arboActuelle + "." + queueName + DEFAULT + ", ")
                        else:
                            sys.stdout.write("Arbo : " + CYAN + queueName + DEFAULT + ", ")
                print("")
            row += 1


# --------------------------------------------#
#                     Code                    #
# --------------------------------------------#

# --------------------------------------------#
#               Affiche la version            #
# --------------------------------------------#

def programVersion():
    print("Version : 0.0.1")


# --------------------------------------------#
#               Quitte en erreur              #
# --------------------------------------------#

def exitWithError(errorText):
    print("\nERROR : " + errorText + "\n")
    sys.exit(1)


# --------------------------------------------#
#           Lit le fichier JSON               #
#             de configuration                #
# --------------------------------------------#

def fileReaderJSON(fileName):
    global vg_config_root
    global vg_xml2xls
    global vg_xlsConfig
    with open(fileName) as jsonFile:
        jsonData = json.load(jsonFile)
    vg_config_root = jsonData['root']
    vg_xml2xls = jsonData['xml2xls']
    vg_xlsConfig = jsonData['xls-config']


# --------------------------------------------#
#               Lit le fichier CSV            #
# --------------------------------------------#

def fileReaderCSV(fileName, delimiterChar):
    global vg_arguments
    print("\nReading CSV file : " + fileName)
    try:
        fichierCsv = csv.DictReader(open(fileName, "rb"), delimiter=delimiterChar, quotechar='"')
        if vg_arguments['verbose']:
            print("CSV file content : ")
            for ligne in fichierCsv:
                print(ligne)
    except csv.Error as e:
        exitWithError('file %s, line %d: %s' % (fileName, ligne.line_num, e))


# --------------------------------------------#
#               Lit le fichier XML            #
# --------------------------------------------#

def fileReaderXML(fileName):
    global vg_xmlcontent
    global vg_arguments
    print("\nReading XML file : " + fileName)
    vg_xmlcontent = {}
    tree = etree.parse(fileName)
    for prop in tree.iter('property'):
        vg_xmlcontent[prop.find('name').text] = prop.find('value').text
        # print prop.find('name').text + '=' + prop.find('value').text
    if vg_arguments['verbose']:
        print("XML file content : ")
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(vg_xmlcontent)


# --------------------------------------------#
#           Ecrit le fichier XLS              #
# --------------------------------------------#

def fileWriterXLS(nbElementsMax, fileName):
    global vg_queues
    print("Création du fichier excel " + fileName)
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(vg_queues)
    # On fabrique le dictionnaire pour convertir la lettre de colonne
    # en décallage numérique
    columnNumber = {}
    for i in range(0, 26):
        columnNumber[chr(ord('A') + i)] = i
    ligne = 0
    colonne = 0
    # On créé le fichier excel
    workbook = xlsxwriter.Workbook(fileName)
    worksheet = workbook.add_worksheet()
    # On écrit les titres de colonnes
    for propertyName in vg_xml2xls:
        # test si la colonne doit exister dans le fichier xls
        if vg_xml2xls[propertyName]['column']:
            worksheet.write(ligne, columnNumber[vg_xml2xls[propertyName]['column']], vg_xml2xls[propertyName]['columnName'])
    # on écrit le root
    worksheet.write(1, colonne, 'root')
    # on itère sur les queues triées
    for queueName in sorted(vg_queues.keys()):
        ligne += 1
        colonne = 2
        # on écrit le nom de la queue
        worksheet.write(ligne, colonne, queueName)
        # on écrit les valeurs dans les bonnes colonnes
        for value in vg_queues[queueName]:
            worksheet.write(ligne, columnNumber[value], vg_queues[queueName][value])
    # Fermeture du fichier excel
    workbook.close()


# --------------------------------------------#
#           Insert dans la config du CSV      #
#           une colonne avec sa valeur        #
#           en créant un dictionnaire         #
# --------------------------------------------#

def insertQueueValueForCSV(queueName, propertyName, value):
    global vg_xml2xls
    global vg_queues
    print("Création du dictionnaire avec les données lues")
    if propertyName in vg_xml2xls:
        # test si la colonne doit exister dans le fichier xls
        if vg_xml2xls[propertyName]['column']:
            vg_queues[queueName][vg_xml2xls[propertyName]['column']] = value
        else:
            print("WARNING : La propriété " + propertyName + " n'a pas de colonne de renseignée dans le fichier de configuration")
    else:
        # Erreur de configuration
        print("ERROR : La propriété " + propertyName + " n'est pas présente dans le fichier de configuration")


# --------------------------------------------#
#           Analyse la configuration          #
# --------------------------------------------#

def analyseConfigurationFromXML():
    global vg_xmlcontent
    global vg_arguments
    global vg_config_root
    global vg_queues
    print("Analyze XML buffer")
    # Constitution d'un tableau des configurations des queues
    #   queues[queueName] = [configName:value, configName:value]
    # On va mémoriser le nombre max d'élélments
    nbElementsMax = 0
    for cle, value in sorted(vg_xmlcontent.iteritems()):
        # calcul du nombre d'élément de la clé
        # yarn.scheduler.capacity.root.SROM.capacity
        # 1   .2        .3       .4   .5   .6
        print(cle + "->" + value)
        elements = cle.split('.')
        nbElement = len(elements)
        if nbElement > nbElementsMax:
            # On mémorise le nombre max d'élélments
            nbElementsMax = nbElement
        if nbElement == 4:
            # SI on a 4 éléments, c'est une variable générale
            print(str(nbElement) + ": property : " + elements[nbElement - 1])
        elif nbElement == 5:
            # SI on a 5 éléments, c'est une variable par defaut des queues
            print(str(nbElement) + ": " + elements[nbElement - 2] + " : property : " + elements[nbElement - 1])
        elif nbElement > 5:
            # Si on a plus de 5 éléments, donc des queues à 1 ou X niveaux
            # On itère sur les sous niveaux si ils existent
            # for x in range(4, len(elements) - 1):     -----------------------------------------
            #  sys.stdout.write(elements[x] + " - ")    ------- TRAITER LES SOUS QUEUES ICI -----
            # print(" -> " + elements[len(elements) - 1])
            # on insert cette valeur dans la config des queues
            insertQueueValueForCSV(elements[4], elements[len(elements) - 1], value) is not True
        else:
            # impossible, on affiche une erreur
            print("ERROR : Ligne en erreur, nombre d'éléments (" + nbElement + ") dans la clé, incohérents : " + cle)
    return nbElementsMax


# --------------------------------------------#
#        Parse la ligne de commande           #
# --------------------------------------------#

def parseCommandLine():
    global vg_arguments
    parser = argparse.ArgumentParser(
        description='Yarn Queue Manager for setting or reading queues configuration', prog='YarnQueueManager')
    parser.add_argument('-f', '--file', type=str, help='CSV file name processed')
    parser.add_argument('-x', '--xml', type=str, help='XML file name processed')
    parser.add_argument('-e', '--excel', type=str, help='Excel file name for output')
    parser.add_argument('-d', '--delimiter', type=str, help='file name processed')
    parser.add_argument('-v', '--version', action='store_true', default=False, help='print the version')
    parser.add_argument('-V', '--verbose', action='store_true', default=False, help='verbose mode')

    vg_arguments = vars(parser.parse_args())
    print(vg_arguments)
    fileReaderJSON('conf/YarnQueueManager.json')

    if vg_arguments['version']:
        programVersion()
        sys.exit

    if vg_arguments['file'] is not None and vg_arguments['delimiter'] is not None:
        if len(vg_arguments['delimiter']) == 1:
            queues = Queues()
            queues.readXlsFile(vg_arguments['excel'], vg_xlsConfig)
            queues.showQueue()
            # fileReaderCSV(vg_arguments['file'], vg_arguments['delimiter'])
        else:
            print("Arguments : \n" + str(vg_arguments))
            exitWithError("invalid arguments : delimiter must be a character.")
    elif vg_arguments['xml'] is not None and vg_arguments['excel'] is not None:
        # Lecture du fichier XML contenant la configuration en place
        fileReaderXML(vg_arguments['xml'])
        # Analyse la configuration XML
        nbElementsMax = analyseConfigurationFromXML()
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(vg_queues)
        fileWriterXLS(nbElementsMax, vg_arguments['excel'])
    else:
        print("Arguments : \n" + str(vg_arguments))
        exitWithError("invalid arguments : file and delimiter must be defined.")


# --------------------------------------------#
#                     Main                    #
# --------------------------------------------#
def main():
    parseCommandLine()


if __name__ == '__main__':
    reload(sys)  
    sys.setdefaultencoding('utf8')
    main()

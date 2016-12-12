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
import re
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
global vg_configPreRoot
global vg_configRoot
global vg_xlsConfig
global vg_queues
vg_queues = defaultdict(dict)

DEFAULT = '\033[39m'
BLACK = '\033[30m'
RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
CYAN = '\033[96m'
BACK_RED = '\033[41m'
BACK_GREEN = '\033[42m'
BACK_BLUE = '\033[44m'
BACK_CYAN = '\033[46m'
BACK_GRAY = '\033[47m'
BACK_DEFAULT = '\033[49m'

# --------------------------------------------#
#                 Classe Queue                #
# --------------------------------------------#


class Queues():
    """Object for Queues"""

    def __init__(self):
        self.queues = defaultdict(dict)

    def addQueueValue(self, arborescence, queueName, propertyName, value):
        try:
            if(arborescence != ""):
                self.queues['.'.join((arborescence, queueName))][propertyName] = value
            else:
                self.queues[queueName][propertyName] = value
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

    # --------------------------------------------#
    #       Création du fichier XLS à parti de    #
    #                   self                      #
    # --------------------------------------------#

    def queuesToXLS(self, fileXLS, configXLS):
        print(BACK_GRAY + BLACK + "\nCreating XLS file :" + DEFAULT + BACK_DEFAULT + " " + fileXLS)
        # On créé le fichier excel
        workbook = xlsxwriter.Workbook(fileXLS)
        # On créé la feuille
        worksheet = workbook.add_worksheet(configXLS['sheet-name'])
        # On créé le format pour le titre
        titleFormat = workbook.add_format({'bold': True, 'font_name': 'calibri', 'font_color': 'white', 'align': 'center', 'valign': 'vcenter', 'bg_color': '#198A8A'})
        worksheet.set_row(int(configXLS['row-titles']), 45, titleFormat)
        # On écrit les titres de colonnes
        for columnLetter in sorted(configXLS['topologie'].keys()):
            # test si la colonne doit exister dans le fichier xls
            if 'columnTitle' in configXLS['topologie'][columnLetter]:
                worksheet.write(int(configXLS['row-titles']), self.lettreVersCol(columnLetter), configXLS['topologie'][columnLetter]['columnTitle'], titleFormat)
        # on écrit le root
        worksheet.write(int(configXLS['row-titles']) + 1, 1, 'root')
        ligne = int(configXLS['cellule-origine']['row'])
        column = int(configXLS['cellule-origine']['col'])
        # On inverse le dictionnaire de la configuration pour se placer avec la propriété
        revertedConf = self.revertConfigurationDict(configXLS['topologie'])
        # on itère sur les queues triées
        for queueName in sorted(self.queues.keys()):

            # TODOOOOOOOOOOO : Gérer les arborescences

            # on écrit le nom de la queue
            worksheet.write(ligne, column, queueName)
            # on écrit les valeurs dans les bonnes colonnes
            for propertyName in self.queues[queueName]:
                # On teste si la propriété est attendue dans le fichier XLS
                if propertyName in revertedConf:
                    worksheet.write(ligne, self.lettreVersCol(revertedConf[propertyName]['column']), self.queues[queueName][propertyName])
            ligne += 1
        # Fermeture du fichier excel
        workbook.close()

    # --------------------------------------------#
    #       Affiche la configuration des queues   #
    # --------------------------------------------#

    def showQueue(self):
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(dict(self.queues))

    # --------------------------------------------#
    #    Inverse le dictionnaire de configuration #
    # --------------------------------------------#
    @staticmethod
    def revertConfigurationDict(configDict):
        tempoDict = {}
        for columns in configDict:
            tempoDict[configDict[columns]['property']] = {"column": columns, "default": configDict[columns]['default']}
        return tempoDict

    # --------------------------------------------#
    #       Converti la LETTRE vers code ASCII    #
    # --------------------------------------------#

    @staticmethod
    def lettreVersNum(lettre):
        return ord(lettre)

    # --------------------------------------------#
    #       Converti la LETTRE en COLONNE NUM     #
    # --------------------------------------------#

    @staticmethod
    def lettreVersCol(lettre):
        if(len(lettre) == 1):
            return (ord(lettre.upper()) - ord('A')) + 1

    # --------------------------------------------#
    #      Converti la COLONNE NUM en LETTRE      #
    # --------------------------------------------#

    @staticmethod
    def colVersLettre(column):
        if(type(column) == int):
            return chr(column + ord('A') - 1)

    # --------------------------------------------#
    #       Converti le code ASCII en LETTRE      #
    # --------------------------------------------#

    @staticmethod
    def numVersLettre(chiffre):
        if (chiffre >= ord('a') and chiffre <= ord('z')) or (chiffre >= ord('A') and chiffre <= ord('Z')):
            return chr(chiffre)

    # --------------------------------------------#
    #               Lit le fichier XLS            #
    # --------------------------------------------#

    def readXlsFile(self, fileXLS, configXLS):
        print(BACK_GRAY + BLACK + "\nReading XLS file :" + DEFAULT + BACK_DEFAULT + " " + fileXLS)
        wb = load_workbook(fileXLS, data_only=True)
        ws = wb.get_sheet_by_name(configXLS['sheet-name'])
        row = int(configXLS['cellule-origine']['row'])
        col = int(configXLS['cellule-origine']['col'])
        rowMax = int(configXLS['row-max'])
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
                # print('Queue trouvée :' + queueName)
                # On itère sur la ligne avec les colonnes configurées
                for column in sorted(configXLS['topologie']):
                    # memorisation de la valeur de la cellule
                    cellValue = str(ws.cell(row=row, column=self.lettreVersCol(column)).value)
                    # Si on est pas sur la queue et sur une colonne après les queues ( hors arborescence )
                    if(column.upper() != self.colVersLettre(int(configXLS['queues-name-column'])) and column.upper() > self.colVersLettre(int(configXLS['queues-name-column']))):
                        # on a joute la donnée si elle est présente, sinon on ajoute la valeur par défaut
                        if(cellValue != 'None'):
                            self.addQueueValue(arboActuelle, queueName, configXLS['topologie'][column]['property'], cellValue)
                            sys.stdout.write(GREEN + configXLS['topologie'][column]['property'] + ": " + cellValue + ", " + DEFAULT)
                        # valeur par defaut
                        elif(configXLS['topologie'][column]['default'] != ""):
                            self.addQueueValue(arboActuelle, queueName, configXLS['topologie'][column]['property'], configXLS['topologie'][column]['default'])
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
    #               Lit le fichier XML            #
    # --------------------------------------------#

    def readXmlFile(self, fileXML, configXML, configPreRoot, configRoot):
        print(BACK_GRAY + BLACK + "\nReading XML file :" + DEFAULT + BACK_DEFAULT + " " + fileXML)
        # On prépare les expression régulières pour matcher root ou preroot
        regPreRoot = re.compile(r"^" + re.escape(configPreRoot))
        regRoot = re.compile(r"^" + re.escape(configRoot))
        tree = etree.parse(fileXML)
        # On itère sur les données du fichier XML
        for prop in tree.iter('property'):
            # On split la chaine en éléments
            # calcul du nombre d'élément de la clé
            # yarn.scheduler.capacity.root.SROM.capacity
            # 1   .2        .3       .4   .5   .6
            elements = prop.find('name').text.split('.')
            nbElements = len(elements)
            if(regRoot.match(prop.find('name').text) is not None):
                # On a trouvé une propriété avec le root
                if(nbElements > 5):
                    # On ajoute la queue avec son arbo et sa valeur
                    # On fait un join des éléments de l'arborescence, on extrait la queue, la propriété et la valeur
                    self.addQueueValue('.'.join(elements[4:-2]), str(elements[-2]), str(elements[-1]), prop.find('value').text)
                else:
                    print(str(nbElements) + " elements : " + prop.find('name').text)
            elif(regPreRoot.match(prop.find('name').text) is not None):
                # On a trouvé une propriété sans le root
                if(nbElements == 4):
                    print(str(nbElements) + " elements : " + prop.find('name').text)
                elif(nbElements == 5):
                    print(str(nbElements) + " elements : " + prop.find('name').text)
                else:
                    print(BACK_RED + BLACK + "Propriété non traitée :" + DEFAULT + BACK_DEFAULT + " " + prop.find('name').text)
            else:
                print(BACK_RED + BLACK + "Propriété non traitée :" + DEFAULT + BACK_DEFAULT + " " + prop.find('name').text)

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
    global vg_configPreRoot
    global vg_configRoot
    global vg_xlsConfig
    with open(fileName) as jsonFile:
        jsonData = json.load(jsonFile)
    vg_configRoot = jsonData['root']
    vg_configPreRoot = jsonData['pre-root']
    vg_xlsConfig = jsonData['xls-config']


# --------------------------------------------#
#        Parse la ligne de commande           #
# --------------------------------------------#

def parseCommandLine():
    global vg_arguments
    parser = argparse.ArgumentParser(
        description='Yarn Queue Manager for setting or reading queues configuration', prog='YarnQueueManager')
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

    if vg_arguments['xml'] is None and vg_arguments['excel'] is not None:
        # Lecture du fichier XLS
        queues = Queues()
        queues.readXlsFile(vg_arguments['excel'], vg_xlsConfig)
        queues.showQueue()
    elif vg_arguments['xml'] is not None and vg_arguments['excel'] is not None:
        # Lecture du fichier XML contenant la configuration en place
        queues = Queues()
        queues.readXmlFile(vg_arguments['xml'], vg_xlsConfig, vg_configPreRoot, vg_configRoot)
        queues.showQueue()
        queues.queuesToXLS(vg_arguments['excel'], vg_xlsConfig)
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

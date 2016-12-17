#!/usr/bin/python
# coding: utf-8

# ===================================================================#
# -------------------------------------------------------------------#
#                         YarnQueueManager                           #
# -------------------------------------------------------------------#
# *******************************************************************#
#                   Eric Deleforterie - 2016/11/21                   #
# -------------------------------------------------------------------#
#                          Notes/Comments                            #
#                                                                    #
# -------------------------------------------------------------------#
#                             HISTORY                                #
#    V0.0.1    Eric Deleforterie - 2016/11/21                        #
#              Creation and first features                           #
# ===================================================================#


# --------------------------------------------#
#             Packages Importation            #
# --------------------------------------------#
from __future__ import print_function
import sys
import re
import json
import pprint
import argparse
import xlsxwriter
from openpyxl import load_workbook
from lxml import etree
from collections import defaultdict

# --------------------------------------------#
#              Variables declaration          #
# --------------------------------------------#
global vg_fileName
global vg_arguments
global vg_delimiter
global vg_xmlcontent
global vg_configuration
global vg_configRoot
global vg_configProperties
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
BACK_YELLOW = '\033[43m'
BACK_BLUE = '\033[44m'
BACK_CYAN = '\033[46m'
BACK_GRAY = '\033[47m'
BACK_DEFAULT = '\033[49m'

# --------------------------------------------#
#                 Queue Class                 #
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
    #       Create the XLS file from the Queues   #
    #                   object                    #
    # --------------------------------------------#

    def queuesToXLS(self, fileXLS, configXLS, configuration):
        print(BACK_GRAY + BLACK + "\nCreating XLS file :" + DEFAULT + BACK_DEFAULT + " " + fileXLS)
        # Create the XLS file
        workbook = xlsxwriter.Workbook(fileXLS)
        # Create the sheet
        worksheet = workbook.add_worksheet(configXLS['sheet-name'])
        # Some paint for the titles
        titleFormat = workbook.add_format({'bold': True, 'font_name': 'calibri', 'font_color': 'white', 'align': 'center', 'valign': 'vcenter', 'bg_color': '#198A8A'})
        worksheet.set_row(int(configXLS['row-titles']), 45, titleFormat)
        # Write column titles
        for columnLetter in sorted(configXLS['topology'].keys()):
            # Check if the column have to appear in the excel file
            if 'columnTitle' in configXLS['topology'][columnLetter]:
                worksheet.write(int(configXLS['row-titles']), self.lettreVersCol(columnLetter), configXLS['topology'][columnLetter]['columnTitle'], titleFormat)
        # Write the root
        worksheet.write(int(configXLS['row-titles']) + 1, 1, str(configuration['root-name']))
        ligne = int(configXLS['cellule-origine']['row'])
        column = int(configXLS['cellule-origine']['col'])
        # Revert topology's configuration for property access easier
        revertedConf = self.revertConfigurationDict(configXLS['topology'])
        # Iterat on sorted queues
        for queueName in sorted(self.queues.keys()):
            if(queueName != configuration['root-name'] and queueName != configuration['general']):

                # TODOOOOOOOOOOO : arborescences management

                # Wrtie queue name
                worksheet.write(ligne, column, queueName)
                # Iterate on column
                for propertyName in self.queues[queueName]:
                    # Check a last one if the property have to be in the XLS file
                    if propertyName in revertedConf:
                        # write the value in the column
                        worksheet.write(ligne, self.lettreVersCol(revertedConf[propertyName]['column']), self.queues[queueName][propertyName])
                ligne += 1
        # XLS file closure
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
    #               Read the XLS file             #
    # --------------------------------------------#

    def readXlsFile(self, fileXLS, configXLS):
        print(BACK_GRAY + BLACK + "\nReading XLS file :" + DEFAULT + BACK_DEFAULT + " " + fileXLS)
        wb = load_workbook(fileXLS, data_only=True)
        ws = wb.get_sheet_by_name(configXLS['sheet-name'])
        row = int(configXLS['cellule-origine']['row'])
        col = int(configXLS['cellule-origine']['col'])
        rowMax = int(configXLS['row-max'])
        # Check the column titles if they are same as the configuration
        for column in configXLS['topology']:
            if(ws[column + configXLS['row-titles']].value != configXLS['topology'][column]['columnTitle']):
                # Error on a column chack
                print(RED + "ERROR : column analyse of the excel file is incoherent in the cell " + column + configXLS['row-titles'] +
                      " find : " + ws[column + configXLS['row-titles']].value + " instead of : " + configXLS['topology'][column]['columnTitle'] + DEFAULT)
                return False
        # parsing file
        actualArborescence = ""
        while row != rowMax:
            if(ws.cell(row=row, column=col).value is None and ws.cell(row=row, column=col + 1).value is None):
                # Epty line, arborescence is reinitialized
                actualArborescence = ""
            elif(ws.cell(row=row, column=col).value is not None and ws.cell(row=row, column=int(configXLS['queues-name-column'])).value is None):
                # New arborescence
                actualArborescence = ws.cell(row=row, column=col).value
                print(BACK_BLUE + CYAN + "New ARBORESCENCE : " + actualArborescence + DEFAULT + BACK_DEFAULT)
            elif(ws.cell(row=row, column=int(configXLS['queues-name-column'])).value is not None):
                # Find a Queue
                queueName = str(ws.cell(row=row, column=int(configXLS['queues-name-column'])).value)
                # Iterate on the line with the confiured columns
                for column in sorted(configXLS['topology']):
                    # Store the cell value
                    cellValue = str(ws.cell(row=row, column=self.lettreVersCol(column)).value)
                    # Check that we are not in the Queue column and on the right of the Queue column
                    if(column.upper() != self.colVersLettre(int(configXLS['queues-name-column'])) and column.upper() > self.colVersLettre(int(configXLS['queues-name-column']))):
                        # Add the value if present, in other case, the default value if in configuration file
                        if(cellValue != 'None'):
                            self.addQueueValue(actualArborescence, queueName, configXLS['topology'][column]['property'], cellValue)
                            sys.stdout.write(GREEN + configXLS['topology'][column]['property'] + ": " + cellValue + ", " + DEFAULT)
                        # default value
                        elif(configXLS['topology'][column]['default'] != ""):
                            self.addQueueValue(actualArborescence, queueName, configXLS['topology'][column]['property'], configXLS['topology'][column]['default'])
                            sys.stdout.write(YELLOW + configXLS['topology'][column]['property'] + ": " + cellValue + ", " + DEFAULT)
                        # no default value and cell empty, do nothing
                        else:
                            sys.stdout.write(RED + configXLS['topology'][column]['property'] + ": " + cellValue + ", " + DEFAULT)
                    # Queue column
                    elif(column.upper() == self.colVersLettre(int(configXLS['queues-name-column']))):
                        if(actualArborescence != ""):
                            sys.stdout.write("Arborescence : " + CYAN + actualArborescence + "." + queueName + DEFAULT + ", ")
                        else:
                            sys.stdout.write("Arborescence : " + CYAN + queueName + DEFAULT + ", ")
                print("")
            row += 1

    # --------------------------------------------#
    #               Read the XML file             #
    # --------------------------------------------#

    def readXmlFile(self, fileXML, configXML, configuration, configProperties):
        print(BACK_GRAY + BLACK + "\nReading XML file :" + DEFAULT + BACK_DEFAULT + " " + fileXML)
        # Regular expression compilation for root or preroot
        regPreRoot = re.compile(r"^" + re.escape(configuration['pre-root']))
        regRoot = re.compile(r"^" + re.escape(configuration['root']))
        tree = etree.parse(fileXML)
        # Iterate on the XML file data
        for prop in tree.iter('property'):
            # Split the string in elements
            # Compute number of elements in the key
            # yarn.scheduler.capacity.root.SROM.capacity
            # 1   .2        .3       .4   .5   .6
            elements = prop.find('name').text.split('.')
            nbElements = len(elements)
            if(regRoot.match(prop.find('name').text) is not None):
                # Find a property with the root
                if(str(elements[-1]) not in configProperties.keys()):
                    # We find a property that is not in  the configuration file properties-config part
                    print(BACK_YELLOW + BLACK + "Property not in properties-config section :" + DEFAULT + BACK_DEFAULT + " " + str(elements[-1]) +
                          " for : " + prop.find('name').text)
                else:
                    if(nbElements > 5):
                        # Add the queue with its arborescence and the value
                        # We join the arborescence elements, extract the queue, the property and the value
                        self.addQueueValue('.'.join(elements[4:-2]), str(elements[-2]), str(elements[-1]), prop.find('value').text)
                    else:
                        # Add to root the general config
                        self.addQueueValue("", str(elements[-2]), str(elements[-1]), prop.find('value').text)
            elif(regPreRoot.match(prop.find('name').text) is not None):
                # Find a property without the root, add to the general configuration Queue
                if(nbElements == 4):
                    self.addQueueValue("", str(configuration['general']), str(elements[-1]), prop.find('value').text)
                # Find a property with a particular structure
                # ex : yarn.scheduler.capacity.queue-mappings-override.enable : false
                # make a dict
                elif(nbElements == 5):
                    self.addQueueValue("", str(configuration['general']), str(elements[-2]), {str(elements[-1]): prop.find('value').text})
                else:
                    print(BACK_RED + BLACK + "Property not traited :" + DEFAULT + BACK_DEFAULT + " " + prop.find('name').text)
            else:
                print(BACK_RED + BLACK + "Property not traited :" + DEFAULT + BACK_DEFAULT + " " + prop.find('name').text)

# --------------------------------------------#
#                   Code                      #
# --------------------------------------------#

# --------------------------------------------#
#               Show the version              #
# --------------------------------------------#


def programVersion():
    print("Version : 0.0.1")


# --------------------------------------------#
#               Exit with Error               #
# --------------------------------------------#

def exitWithError(errorText):
    print("\nERROR : " + errorText + "\n")
    sys.exit(1)


# --------------------------------------------#
#           Read the configuration            #
#                  JSON file                  #
# --------------------------------------------#

def fileReaderJSON(fileName):
    global vg_configuration
    global vg_configProperties
    global vg_xlsConfig
    with open(fileName) as jsonFile:
        jsonData = json.load(jsonFile)
    vg_configuration = jsonData['configuration']
    vg_configProperties = jsonData['properties-config']
    vg_xlsConfig = jsonData['xls-config']


# --------------------------------------------#
#             Command line parsing            #
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
        # Read the XLS File and show the queues
        queues = Queues()
        queues.readXlsFile(vg_arguments['excel'], vg_xlsConfig)
        queues.showQueue()
    elif vg_arguments['xml'] is not None and vg_arguments['excel'] is not None:
        # Read the XML file with the actual configuration and generate the XLS file
        queues = Queues()
        queues.readXmlFile(vg_arguments['xml'], vg_xlsConfig, vg_configuration, vg_configProperties)
        queues.showQueue()
        queues.queuesToXLS(vg_arguments['excel'], vg_xlsConfig, vg_configuration)
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

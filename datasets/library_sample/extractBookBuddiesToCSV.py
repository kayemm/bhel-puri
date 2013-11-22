import xml.etree.ElementTree as ET
import sys
"""
This script extracts recommendation data from the suggestions dataset provided
by the University of Huddersfield. The original data is provided in XML format 
with lots of other information. The purpose of this script is to extract the 
relevant data from the xml file to build a recommendation algorithm based on
this data. 
The element of interest in the XML file is "Item". An item is a book that has
been issued at least once. The Item element has the following tags:
    1. ISBN
    2. URL
    3. Title
    4. Suggestions
    Suggestions contains one or more "isbn" tags. Each IBSN tag represents
    a book recommendation that would go along with the book recommended
    by item. the script extracts the attribute "commom" from each of these
    isbn tags and the text of the tag (representing the isbn of the
    recommended book)
The output of the script is a csv file. The element of each line is the
ISBN of a book followed by a list of books that are suggestions based
on circulation data. These books have a direct connection (not friend
of a friend type) between them. 

  
"""
fileName = sys.argv[1]
xmlTree = ET.parse(fileName)
treeRoot = xmlTree.getroot()
comma = ","
for items in treeRoot:
    if (items.tag == "items"):
        for item in items:
            isbn1 = ""
            if (item.tag == "item"):
                for field in item:
                    if (field.tag == "isbn"):
                        isbn1 = field.text
                        str = isbn1+comma
                for suggestions in item:
                    if (field.tag == "suggestions"):
                        for isbns in suggestions:
                            if (isbns.tag == "isbn"):
                                isbn2 = isbns.text
                                #confidence = isbns.attrib["common"]
                                str += isbn2+comma
                print str[0:len(str)-1]
                str = ""
            
    
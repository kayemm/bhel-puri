import xml.etree.ElementTree as ET
import sys
"""
The scripts extracts the ISBN and title of books from circulation
dataset. The o/p of the script is  csv file of tuples. Each tuple
contains the ISBN of a book and associated title of a book.

XML file structure
<Circulation Data>
  <license>...</license>
  <items>
    <item>...</item>
           :
           :
    <item>...</item>           
  </items>
</Circulation Data>

We will be processing the item contents within items.
Structure of Item:
  <item>
    <title>..
    <copies>
    <isbn>
    <url>
    <loan_history>
  </item>

What we need are the title and isbn tags from item
"""
# List to store ISBNs extracted so far. Once an ISBN has been read, it will not
# be processed again though it may appear in the circulation data set.
isbnList = []
fileName = sys.argv[1]
xmlTree = ET.parse(fileName)
treeRoot = xmlTree.getroot()
for items in treeRoot:
    if (items.tag == "items"):
        for item in items:
            isbn = ""
            title = ""
            if (item.tag == "item"):
                for field in item:
                    if (field.tag == "isbn"):
                        if field.text not in isbnList:
                            isbnList.append(field.text)
                            isbn = field.text
                    if (field.tag == "title"):
                        # remove the trailing " /"
                        titleLen = len(field.text)
                        title = field.text[0:titleLen-2]
                        title = title.replace(","," ")
                        title = title.encode('utf-8')
                        
                print isbn + "," + title    
                
            
    
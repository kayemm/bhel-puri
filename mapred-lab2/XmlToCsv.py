import xml.etree.ElementTree as ET
import sys
"""
This is a helper application to convert the XML files of the stack overflow 
dump into csv format. 
"""
def extractPostsXML(treeRoot):
    # We extract only some of the attributes from the XML record.
    xmlAttributes = ['Id',"PostTypeId",'Score','ViewCount','OwnerUserId','AnswerCount','CommentCount','AcceptedAnswerId','CommentsCount']
    
    # Print the header row. The table name should be the first column.
    hdrStr = "TableName,"
    for xmlAtt in xmlAttributes:
        hdrStr += xmlAtt + ","
    hdrStr += "Title"
    print hdrStr
    
    # Iterate over each node and print the values    
    for child in treeRoot:
        # attrib is a dictionary.
        attrib = child.attrib
        csvStr = "POSTS,"
        for xmlAtt in xmlAttributes:
            if xmlAtt in attrib:
                csvStr += attrib[xmlAtt]
            csvStr += ","
        
        # Some special processing for the title attribute
        # Remove any commas in the text of the title. 
        if 'Title' in attrib:
            titleStr = attrib['Title']
            csvStr += titleStr.replace(',',' ')
        
        print csvStr

def extractUsersXML(treeRoot):
    # We extract only some of the attributes from the XML record.
    xmlAttributes = ['Id','Reputation','DisplayName','UpVotes','DownVotes']
    
    # Print the header row. The table name should be the first column.
    hdrStr = "TableName,"
    for xmlAtt in xmlAttributes:
        # There should be no comma after the last attribute
        if xmlAtt <> "DownVotes":
            hdrStr += xmlAtt + ","
        else:
            hdrStr += xmlAtt
    print hdrStr
    
    # Iterate over each node and print the values
    for child in treeRoot:
        # attrib is a dictionary.
        attrib = child.attrib
        csvStr = "USERS,"
        for xmlAtt in xmlAttributes:
            if xmlAtt in attrib:
                # No comma after the last attribute
                if xmlAtt <> 'DownVotes':
                    csvStr += attrib[xmlAtt]
                    csvStr += ","
                else:
                    csvStr += attrib[xmlAtt]
        print csvStr

if __name__ == '__main__':
    fileName = sys.argv[1]
    xmlTree = ET.parse(fileName)
    treeRoot = xmlTree.getroot()
    if (fileName.endswith('Posts_Sample.xml')):
        extractPostsXML(treeRoot)
    elif (fileName.endswith('Users_Sample.xml')):    
        extractUsersXML(treeRoot)


import xml.etree.ElementTree as ET

list_of_tags = [
    "datastagejob",
    "jobname",
    "description",
    "stages",
    "stage",
    "stagetype",
    "name",
    "properties",
    "filepath",
    "fielddefinitions",
    "inputfields",
    "outputfields",
    "transformations",
    "transformation",
    "datatype",
    "length",
    "field",
    "links",
    "link",
    "from",
    "to"

]

tree = ET.parse("datastage_sample 2.dsx")

root = tree.getroot()

print(root.tag)
print("\n\n")
for child in root:
    print(child.tag, "\t:\t", child.text.strip())
    for childs in child:
        print(childs.tag)
        for child_ in childs:
            print("\t", child_.tag)
            for childs_ in child_:
                print("\t\t", childs_.tag)
                for childe in childs_:
                    print("\t\t\taaaaaa", childe.tag)
                    for chil in childe:
                        print("\t\t\t\t", chil.tag)

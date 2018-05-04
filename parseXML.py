import xml.etree.ElementTree as etree


tree = etree.parse('Ver (3.3).xml')
root = tree.getroot()
print(root.tag)
# print(root.tag)
# print(len(root))
for a in root.attrib:
    print(a, root.attrib[a])

for child in root:
    print(child.tag)
    for a in child.attrib:
        print(a, child.attrib[a])
    for sub_child in child:
        print('  - ', sub_child.tag)
        for a in sub_child.attrib:
            print('    ', a, sub_child.attrib[a])
        for sub_sub_child in sub_child:
            print('    + ', sub_sub_child.tag)
            for a in sub_sub_child.attrib:
                print('      ', a, sub_sub_child.attrib[a])

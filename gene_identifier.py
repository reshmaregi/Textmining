import xml.etree.ElementTree as ET
import re
import csv


def inter():
    gene_info = []
    Article= []
    print("Article Title")
    tree = ET.parse('C://Users//reshma.regi//Desktop//pubmed_temp//pubmed18n0065.xml')
    doc = tree.getroot()
    for ArticleTitle in doc.iter('ArticleTitle'):
        file1 = (ET.tostring(ArticleTitle, encoding='utf8').decode('utf8'))
        filename = file1[52:(len(file1))]
        Article = filename.split("<")[0]
        # print(type(Article))
        title = Article.split()
        gene_list = ["ABCD1","ADA","ALDOB","APC","ARSB","ATAD3B","AXIN2","BLM","BMPR1A","BRAF","BRCA1" ,"BRCA2",
                     "BRIP1","BTD","CDH1","CDKN2A","CLCN6","COL3A1","CYP21A2","DSC2","DSG2","EPCAM","ERBB2","EYS",
                     "F11","FBN1","GJB2","GP6","HBB","HEXA","HEXB","HSD3B2","HSPG2","KRAS","LRPPRC","MEFV","MUT",
                     "NPC1","PAH","PDHA1","PKHD1","RMRP","SLC26A4","SMN1","SMPD1","TP53"]
        gene_list= '|'.join(gene_list)
        pos = 0
        for i in title:
            m = re.search(gene_list, i)
            if m:
                gene = m.group(0)
                start = gene_list.find(gene)
                end = start + len(gene)
                # print(start, end, gene, pos, Article)
                gene_info=[start, end, gene, pos, Article]
                print(gene_info)
                with open('C://Users//reshma.regi//PycharmProjects//text_mining//test_gene.csv', 'a+') as myfile:
                    wr = csv.writer(myfile, lineterminator='\n')
                    wr.writerow(gene_info)

            pos += 1




if __name__ == '__main__':
    inter() #test
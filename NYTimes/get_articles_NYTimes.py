import requests
import re

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

def string_process(string):
    a = 0
    while True:
        a = string.find('<',a)
        if a == -1:
            break
        b = string.find('>',a)
        string =string[0:a]+string[b+1:]
    return string
    

def get_url_list(url):
    html = open_url(url)
    urllist1 = list(set(re.findall(r'<a data-rref="" href="(.+?)">',html)))
    urllist2 = list(set(re.findall(r'<a href="(/.+?)"',html)))
    urllist = urllist1 + urllist2
    return urllist
    
def open_url(url):
    response = requests.get(url,headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'})
    response.encoding = 'utf-8'
    html = response.content.decode('utf-8')
    return html
    
def get_content(html):
    title = re.findall(r'<h1.*?>(.+?)</h1>',html)
    summary = re.findall(r'<p id="article-summary" class="css-w6ymp8 e1wiw3jv0">(.+?)</p>',html)
    content_list = re.findall(r'<p class="css-158dogj evys1bk0">(.+?)</p>',html)
    all_content = title + summary + content_list
    return all_content

def save_content(content, num, date):
    with open(r'%s.txt' % date,'a',encoding='utf-8') as f:
        f.write("No.%d article:\n" % num)
        for each_line in content:
            if '<' in each_line:
                each_line = string_process(each_line)
            f.write(each_line + '\n\n')
        f.write("\n\n\n\n")
        

if __name__ == '__main__':
    the_year = input("Please enter the year:")
    the_month = input("Please enter the month:")
    start = input("Please enter the start day:")
    end = input("Please enter the end day:")
    for the_day in range(int(start),int(end)+1):
        the_day = str(the_day)
        if len(the_day) == 1:
            the_day = '0' + the_day
        url = 'https://www.nytimes.com/issue/todayspaper/%s/%s/%s/todays-new-york-times' % (the_year,the_month,the_day)
        print('Date:%s/%s/%s loading......' % (the_year,the_month,the_day))
        num =1
        for each_url in get_url_list(url):
            each_url = "https://www.nytimes.com" + each_url
            content = get_content(open_url(each_url))
            print("Loading No.%d article......" % num)
            the_date = "NYTimes_%s_%s_%s" % (the_year,the_month,the_day)
            save_content(content, num, the_date)
            num += 1
        print('%s/%s/%s finished!\n' % (the_year,the_month,the_day))

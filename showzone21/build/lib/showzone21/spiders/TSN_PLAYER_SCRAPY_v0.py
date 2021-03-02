import scrapy
from datetime import datetime
import pandas as pd
import statistics

class TSNSpider(scrapy.Spider):
    startTime = datetime.now()
    
    name = 'TSNSpider'
    
    custom_settings = {
        'CONCURRENT_REQUESTS': 100,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 100,
        'LOG_ENABLED': True
    }
    
    allowed_domains = ['mlb20.theshow.com']

    def start_requests(self):
        colnames = ['','ID','LINK','CARD','OVR','SERIES','SERIES.SHORT','NAME','POS','TEAM','LINK.RARE']
        data = pd.read_csv('all_player_urls_df.csv',names=colnames)
        urls = data.LINK.tolist()[1:]
        
        for url in urls:
            yield scrapy.Request(url=url,callback=self.parse)
        
    def parse(self, response):

        print("processing:" + response.url)
        
        if(len(response.xpath("//div[contains(.//text(), 'H/9')]/following-sibling::div[1]/text()").extract()) > 0):
            sta = response.xpath("//div[contains(.//text(), 'STA')]/following-sibling::div[1]/text()").extract()
            hp9 = response.xpath("//div[contains(.//text(), 'H/9')]/following-sibling::div[1]/text()").extract()
            kp9 = response.xpath("//div[contains(.//text(), 'K/9')]/following-sibling::div[1]/text()").extract()
            bbp9 = response.xpath("//div[contains(.//text(), 'BB/9')]/following-sibling::div[1]/text()").extract()
            hrp9 = response.xpath("//div[contains(.//text(), 'HR/9')]/following-sibling::div[1]/text()").extract()
            pclu = [response.xpath("//div[contains(.//text(), 'CLU')]/following-sibling::div[1]/text()").extract()[0]]
            ctrl = response.xpath("//div[contains(.//text(), 'CTRL')]/following-sibling::div[1]/text()").extract()
            vel = response.xpath("//div[contains(.//text(), 'VEL')]/following-sibling::div[1]/text()").extract()
            brk = response.xpath("//div[contains(.//text(), 'BRK')]/following-sibling::div[1]/text()").extract()
            bclu = [response.xpath("//div[contains(.//text(), 'CLU')]/following-sibling::div[1]/text()").extract()[1]]
        else:
            sta = ['0']
            hp9 = ['0']
            kp9 = ['0']
            bbp9 = ['0']
            hrp9 = ['0']
            pclu = ['0']
            ctrl = ['0']
            vel = ['0']
            brk = ['0']
            bclu = [response.xpath("//div[contains(.//text(), 'CLU')]/following-sibling::div[1]/text()").extract()[0]]
        
        if(len(response.xpath("//div[contains(.//text(), 'BLK')]/following-sibling::div[1]/text()").extract()) > 0):
            blk = response.xpath("//div[contains(.//text(), 'BLK')]/following-sibling::div[1]/text()").extract()
        else:
            blk = ['0']
        
        if (len(response.css('.mlb20-card-event::text')) > 0):
            event = "Y"
        else:
            event = "N"
        
        if (len(response.css('h2::text').extract()) > 0):
            market = "Yes"
            
            #sales
            if (len(response.xpath('//table[@id="table-completed-orders"]/tbody/tr')) == 0):
                salesmin = 0
                lwkavgbuy = 0
                lwkavgsell = 0
                buy_estimate = 300000
            else: 
                numsales = len(response.xpath('//table[@id="table-completed-orders"]/tbody/tr'))
                latestsale = pd.to_datetime(response.xpath('//table[@id="table-completed-orders"]/tbody/tr')[1].xpath('td//text()')[2].extract(),infer_datetime_format=True)
                oldestsale = pd.to_datetime(response.xpath('//table[@id="table-completed-orders"]/tbody/tr')[-1].xpath('td//text()')[2].extract(),infer_datetime_format=True)
                salesmin = numsales/((latestsale-oldestsale).total_seconds()/60)
                
                if (len(pd.Series(response.xpath('//table[@id="table-completed-orders"]/tbody/tr')[:5].xpath('td//text()').extract())) > 14):
                    price_indices = [1,4,7,10,13]
                    price_series = pd.Series(response.xpath('//table[@id="table-completed-orders"]/tbody/tr')[:5].xpath('td//text()').extract())
                    price_accessed = price_series[price_indices]
                    price_list = list(price_accessed)
                    price_list = [s.strip() for s in price_list]
                    price_list = [int(i.replace(',','')) for i in price_list]
                    buy_estimate = int(statistics.mean(price_list))
                else:
                    buy_estimate = 0
                
                #trends
                if (len(pd.Series(response.xpath('//table[@id="table-trends"]/tbody/tr')[:7].xpath('td//text()').extract())) < 35):
                    lwkavgbuy = 0
                    lwkavgsell = 0
                else:
                    buy_indices = [2,7,12,17,22,27,32]
                    buy_series = pd.Series(response.xpath('//table[@id="table-trends"]/tbody/tr')[:7].xpath('td//text()').extract())
                    buy_accessed = buy_series[buy_indices]
                    buy_list = list(buy_accessed)
                    buy_list = [s.strip() for s in buy_list]
                    buy_list = [int(i) for i in buy_list]
                    lwkavgbuy = statistics.mean(buy_list)
                    
                    sell_indices = [4,9,14,19,24,29,34]
                    sell_series = pd.Series(response.xpath('//table[@id="table-trends"]/tbody/tr')[:7].xpath('td//text()').extract())
                    sell_accessed = sell_series[sell_indices]
                    sell_list = list(sell_accessed)
                    sell_list = [s.strip() for s in sell_list]
                    sell_list = [int(i) for i in sell_list]
                    lwkavgsell = statistics.mean(sell_list)
        else:
            market = "No"
            salesmin = 0
            lwkavgbuy = 0
            lwkavgsell = 0
            buy_estimate = -1
        
        name = response.css('.mlb20-card-firstname::text').extract_first() + " " + response.css('.mlb20-card-lastname::text').extract_first()
        cvr = response.xpath("//div[contains(.//text(), 'CON R')]/following-sibling::div[1]/text()").extract()
        cvl = response.xpath("//div[contains(.//text(), 'CON L')]/following-sibling::div[1]/text()").extract()
        pvr = response.xpath("//div[contains(.//text(), 'PWR R')]/following-sibling::div[1]/text()").extract()
        pvl = response.xpath("//div[contains(.//text(), 'PWR L')]/following-sibling::div[1]/text()").extract()
        vis = response.xpath("//div[contains(.//text(), 'VIS')]/following-sibling::div[1]/text()").extract()
        disc = response.xpath("//div[contains(.//text(), 'DISC')]/following-sibling::div[1]/text()").extract()
        bnt = [response.xpath("//div[contains(.//text(), 'BNT')]/following-sibling::div[1]/text()").extract()[0]]
        drg = response.xpath("//div[contains(.//text(), 'DRG BNT')]/following-sibling::div[1]/text()").extract()
        dur = response.xpath("//div[contains(.//text(), 'DUR')]/following-sibling::div[1]/text()").extract()
        fld = response.xpath("//div[contains(.//text(), 'FLD')]/following-sibling::div[1]/text()").extract()
        arm = response.xpath("//div[contains(.//text(), 'ARM STR')]/following-sibling::div[1]/text()").extract()
        acc = response.xpath("//div[contains(.//text(), 'ARM ACC')]/following-sibling::div[1]/text()").extract()
        reac = response.xpath("//div[contains(.//text(), 'REAC')]/following-sibling::div[1]/text()").extract()
        spd = response.xpath("//div[contains(.//text(), 'SPD')]/following-sibling::div[1]/text()").extract()
        stl = response.xpath("//div[contains(.//text(), 'STL')]/following-sibling::div[1]/text()").extract()
        agg = response.xpath("//div[contains(.//text(), 'BR AGG')]/following-sibling::div[1]/text()").extract()        
        
        yield {
            'snlink': response.url,
            'cardid': response.url[32:],

            'imglink': response.css('.mlb20-card-actionshot').xpath('@src').getall(),
            'rarelink': response.css('.icons-rarity').xpath('@src').extract_first(),
            'logolink': response.css('.mlb20-card-logo').xpath('@src').getall(),
            'ovr': response.css('.mlb20-card-rarity::text').extract_first(),
            'name': name,
            'pos': response.css('.mlb20-card-position::text').extract_first(),
            'hands': response.css('.mlb20-card-hands::text').extract_first(),
            'team': response.css('.mlb20-card-teamname::text').extract_first(),
            'bats': response.css('.flex-table-cell::text').extract()[2],
            'throws': response.css('.flex-table-cell::text').extract()[3],
            
            'jersey': int(response.css('h1::text').extract()[0].splitlines()[1]),

            'sec': response.xpath("//div[contains(.//text(), 'Secondary')]/following-sibling::div[1]/text()").extract(),
            'weight': response.xpath("//div[contains(.//text(), 'Weight')]/following-sibling::div[1]/text()").extract(),
            'height': response.xpath("//div[contains(.//text(), 'Height')]/following-sibling::div[1]/text()").extract(),
            'age': response.xpath("//div[contains(.//text(), 'Age')]/following-sibling::div[1]/text()").extract(),
            'born': response.xpath("//div[contains(.//text(), 'Born')]/following-sibling::div[1]/text()").extract(),
            
            'sta': sta,
            'hp9': hp9,
            'kp9': kp9,
            'bbp9': bbp9,
            'hrp9': hrp9,
            'pclu': pclu,
            'ctrl': ctrl,
            'vel': vel,
            'brk': brk,
            'cvr': cvr,
            'cvl': cvl,
            'pvr': pvr,
            'pvl': pvl,
            'vis': vis,
            'disc': disc,
            'bclu': bclu,
            'bnt': bnt,
            'drg': drg,
            'dur': dur,
            'fld': fld,
            'arm': arm,
            'acc': acc,
            'reac': reac,
            'blk': blk,
            'spd': spd,
            'stl': stl,
            'agg': agg,
            
            'market': market,
            'event': event,
            
            'quirks': response.css('.quirk-item').css('strong::text').extract(),
            
            'salesmin': salesmin,
            'lwkavgbuy': lwkavgbuy,
            'lwkavgsell': lwkavgsell,
            'buy_estimate': buy_estimate,
            'itemid': name.replace(" ", "")+str(sta[0])+str(hp9[0])+str(kp9[0])+str(bbp9[0])+str(hrp9[0])+str(pclu[0])+str(ctrl[0])+str(vel[0])+str(brk[0])+str(cvr[0])+str(cvl[0])+str(pvr[0])+str(pvl[0])+str(vis[0])+str(disc[0])+str(bclu[0])+str(bnt[0])+str(drg[0])+str(fld[0])+str(arm[0])+str(acc[0])+str(reac[0])+str(blk[0])+str(spd[0])+str(stl[0])+str(agg[0])
        }
    
    print(datetime.now() - startTime)
    
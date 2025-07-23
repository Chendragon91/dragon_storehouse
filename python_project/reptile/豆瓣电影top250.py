import asyncio
import json
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode
from bs4 import BeautifulSoup

async def crawl_doulist_page(crawler, url, all_movies):
    """爬取单个页面的电影数据"""
    result = await crawler.arun(url=url, config=run_config)

    if not result.success:
        print(f"页面爬取失败: {url} - {result.error_message}")
        return None, 0

    soup = BeautifulSoup(result.html, 'html.parser')
    movie_items = soup.select('.doulist-item')

    for item in movie_items:
        # 初始化电影数据字典，所有字段默认为null
        movie = {
            "name": None,
            "rating": None,
            "director": None,
            "cast": None,
            "type": None,
            "country": None,
            "year": None
        }

        # 提取电影名称
        name_elem = item.select_one('.title a')
        if name_elem and name_elem.text.strip():
            movie['name'] = name_elem.text.strip()

        # 提取评分
        rating_elem = item.select_one('.rating .rating_nums')
        if rating_elem and rating_elem.text.strip():
            movie['rating'] = rating_elem.text.strip()

        # 提取其他元数据
        abstract = item.select_one('.abstract')
        if abstract:
            for line in abstract.text.split('\n'):
                line = line.strip()
                if not line:
                    continue
                if '导演:' in line:
                    director = line.replace('导演:', '').strip()
                    if director: movie['director'] = director
                elif '主演:' in line:
                    cast = line.replace('主演:', '').strip()
                    if cast: movie['cast'] = cast
                elif '类型:' in line:
                    movie_type = line.replace('类型:', '').strip()
                    if movie_type: movie['type'] = movie_type
                elif '制片国家/地区:' in line:
                    country = line.replace('制片国家/地区:', '').strip()
                    if country: movie['country'] = country
                elif '年份:' in line:
                    year = line.replace('年份:', '').strip()
                    if year: movie['year'] = year

        all_movies.append(movie)

    # 检查是否有下一页
    next_page = soup.select_one('.paginator .next a')
    return next_page['href'] if next_page else None

async def main():
    browser_config = BrowserConfig(verbose=True)
    global run_config
    run_config = CrawlerRunConfig(
        word_count_threshold=10,
        excluded_tags=['form', 'header'],
        exclude_external_links=True,
        process_iframes=True,
        remove_overlay_elements=True,
        cache_mode=CacheMode.ENABLED
    )

    all_movies = []
    base_url = "https://www.douban.com/doulist/3936288/"
    current_url = base_url

    async with AsyncWebCrawler(config=browser_config) as crawler:
        page_num = 1
        while current_url:
            print(f"正在爬取第 {page_num} 页: {current_url}")
            next_page_url = await crawl_doulist_page(crawler, current_url, all_movies)
            if next_page_url:
                current_url = base_url + next_page_url if not next_page_url.startswith('http') else next_page_url
                page_num += 1
                await asyncio.sleep(2)  # 添加延迟避免被封
            else:
                current_url = None

    # 保存数据到JSON文件
    with open('douban_movies.json', 'w', encoding='utf-8') as f:
        json.dump(all_movies, f, ensure_ascii=False, indent=2)

    print(f"爬取完成，共获取 {len(all_movies)} 部电影数据，已保存到 douban_movies.json")

if __name__ == "__main__":
    asyncio.run(main())
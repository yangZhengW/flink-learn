import os
import re
import time
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# 设置请求头，防止被拦截
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}

def create_folder(base_path='downloads'):
    folder_name = f"{base_path}/{int(time.time())}"
    os.makedirs(folder_name, exist_ok=True)
    return folder_name

def get_real_url(url):
    """防止短链，解出真实链接"""
    try:
        resp = requests.head(url, allow_redirects=True, headers=HEADERS, timeout=10)
        return resp.url
    except Exception as e:
        print(f"解短链失败: {e}")
        return url

def download_file(url, save_folder):
    filename = os.path.join(save_folder, os.path.basename(url.split('?')[0]))
    try:
        r = requests.get(url, headers=HEADERS, stream=True, timeout=20)
        if r.status_code == 200:
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            print(f"已下载：{filename}")
    except Exception as e:
        print(f"下载失败：{e}")

def parse_xiaohongshu(url):
    """小红书解析逻辑"""
    html = requests.get(url, headers=HEADERS).text
    media_urls = []

    # 小红书有时候媒体在 script 标签里（用正则提取）
    image_pattern = re.compile(r'"url":"(https:[^"]+?.jpg)"')
    video_pattern = re.compile(r'"video_url":"(https:[^"]+?.mp4)"')

    media_urls += image_pattern.findall(html)
    media_urls += video_pattern.findall(html)

    return media_urls

def parse_douyin(url):
    """抖音解析逻辑"""
    real_url = get_real_url(url)
    html = requests.get(real_url, headers=HEADERS).text

    video_url_match = re.search(r'"playAddr":"(https:\\/\\/[^"]+)"', html)
    media_urls = []

    if video_url_match:
        video_url = video_url_match.group(1).replace('\\u0026', '&').replace('\\/', '/')
        media_urls.append(video_url)

    return media_urls

def parse_taobao(url):
    """淘宝解析逻辑"""
    html = requests.get(url, headers=HEADERS).text
    soup = BeautifulSoup(html, 'html.parser')
    media_urls = []

    # 淘宝商品页一般图片在 img 标签中
    for img in soup.find_all('img'):
        src = img.get('src')
        if src and ('jpg' in src or 'png' in src):
            if src.startswith('//'):
                src = 'https:' + src
            media_urls.append(src)

    return list(set(media_urls))  # 去重一下

def detect_platform(url):
    return 'taobao'
    # if 'xiaohongshu' in url:
    #     return 'xiaohongshu'
    # elif 'douyin' in url:
    #     return 'douyin'
    # elif 'taobao' in url or 'tmall' in url:
    #     return 'taobao'
    # else:
    #     return 'unknown'

def main():
    url = f'''
            https://e.tb.cn/h.6lFa2wW1WjU4I3j?tk=5XyxVew1soI
        '''
    platform = detect_platform(url)

    if platform == 'unknown':
        print("暂不支持这个平台。")
        return

    folder = create_folder()

    if platform == 'xiaohongshu':
        media_list = parse_xiaohongshu(url)
    elif platform == 'douyin':
        media_list = parse_douyin(url)
    elif platform == 'taobao':
        media_list = parse_taobao(url)
    else:
        media_list = []

    if not media_list:
        print("没有找到可下载的媒体资源。")
        return

    for media_url in media_list:
        download_file(media_url, folder)

    print(f"\n下载完成，所有文件保存在：{folder}")

if __name__ == "__main__":
    main()
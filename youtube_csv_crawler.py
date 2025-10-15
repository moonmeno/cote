"""YouTube CSV Crawler
======================

이 스크립트는 YouTube Data API를 활용하여 영상 및 채널 메타데이터를
수집하고 CSV 파일로 저장한다. 복수 개의 API 키를 순환하여 쿼터 초과
상황을 최소화하며, 이미 저장된 CSV 데이터를 다시 불러와 중복을
방지한다. 또한 `--export-titles` 옵션을 사용하면 기존 영상 데이터에서
제목만 추출하여 자연어 처리(NLP)나 키워드 분석에 활용할 수 있다.

사용 예시:

1. 카테고리별 영상 메타데이터 수집

   python youtube_csv_crawler.py --api-keys "KEY1,KEY2,KEY3" --categories 17,10 \
       --per-category 300 --region KR --published-after 2025-01-01T00:00:00Z

2. 저장된 영상 CSV에서 제목만 추출하여 별도 파일로 내보내기

   python youtube_csv_crawler.py --export-titles titles.csv

필수 준비 사항:
- 실제 발급받은 YouTube Data API 키를 `--api-keys` 인자로 전달해야 한다.
- 크롤링 모드를 사용할 때는 조회할 카테고리 ID를 `--categories` 인자로
  지정해야 한다.
"""
import argparse
import csv
import json
import os
import re
import time
from typing import Dict, Iterable, List, Optional, Set

import requests


# 유튜브 API 사용 시 자주 발생하는 쿼터 초과 오류 코드
QUOTA_ERROR_REASONS = {
    "quotaExceeded",
    "dailyLimitExceeded",
    "rateLimitExceeded",
}

ZERO_WIDTH_PATTERN = re.compile(r"[\u200B-\u200D\uFEFF]")


class APIKeyManager:
    """YouTube API 키를 순환하며 제공하는 관리자.

    여러 개의 키를 입력받아 리스트로 저장하고, 요청이 실패할 때마다
    순차적으로 키를 변경하여 재시도를 수행한다.
    """

    def __init__(self, keys: Iterable[str]):
        filtered = [k.strip() for k in keys if k.strip()]
        if not filtered:
            raise ValueError("사용 가능한 API 키가 필요합니다.")
        self._keys: List[str] = filtered
        self._index: int = 0

    @property
    def current_key(self) -> str:
        """현재 선택된 API 키 반환."""
        return self._keys[self._index]

    def rotate(self) -> None:
        """다음 API 키로 전환."""
        self._index = (self._index + 1) % len(self._keys)

    @property
    def total_keys(self) -> int:
        return len(self._keys)


class YouTubeAPIClient:
    """YouTube API 호출을 책임지는 헬퍼 클래스.

    각 API 요청에서 발생하는 HTTP 오류를 감지하고, 쿼터 관련 오류일 경우
    `APIKeyManager`를 통해 키를 회전한다. 또한 백오프(backoff) 간격으로
    재시도를 수행하여 안정적인 수집을 돕는다.
    """

    BASE_URL = "https://www.googleapis.com/youtube/v3"

    def __init__(self, key_manager: APIKeyManager, max_retries: int = 3, backoff: float = 1.5):
        self.key_manager = key_manager
        self.max_retries = max_retries
        self.backoff = backoff
        self.session = requests.Session()

    def request(self, endpoint: str, params: Dict[str, str]) -> Dict:
        """주어진 endpoint로 API 호출을 수행하며 실패 시 재시도 및 키 회전을 처리한다."""
        attempts = 0
        total_attempts = self.key_manager.total_keys * self.max_retries

        while attempts < total_attempts:
            key = self.key_manager.current_key
            merged_params = dict(params)
            merged_params["key"] = key
            url = f"{self.BASE_URL}/{endpoint}"
            try:
                response = self.session.get(url, params=merged_params, timeout=10)
            except requests.RequestException as exc:
                attempts += 1
                print(f"[오류] 네트워크 문제 발생: {exc}. {self.backoff}초 후 재시도합니다.")
                time.sleep(self.backoff)
                continue

            if response.status_code == 200:
                return response.json()

            error_info = self._extract_error(response)
            reason = error_info.get("reason") if error_info else None
            message = error_info.get("message") if error_info else response.text
            print(f"[경고] API 오류 발생 (reason={reason}): {message}")

            if reason in QUOTA_ERROR_REASONS:
                self.key_manager.rotate()
                attempts += 1
                print("[안내] API 키를 회전합니다.")
                time.sleep(self.backoff)
                continue

            if response.status_code in (500, 503):
                attempts += 1
                time.sleep(self.backoff)
                continue

            response.raise_for_status()

        raise RuntimeError("사용 가능한 API 키로 요청을 완료할 수 없습니다.")

    @staticmethod
    def _extract_error(response: requests.Response) -> Optional[Dict[str, str]]:
        try:
            data = response.json()
        except ValueError:
            return None
        errors = data.get("error", {}).get("errors")
        if isinstance(errors, list) and errors:
            return errors[0]
        return data.get("error")


def read_existing_ids(file_path: str, key_field: str) -> Set[str]:
    """기존 CSV에서 특정 키 필드를 읽어와 중복을 방지한다.

    이미 저장된 주키(예: videoId, channelId)를 집합으로 로드하여 이후
    수집 단계에서 동일한 항목이 다시 저장되는 것을 막는다.
    """
    if not os.path.exists(file_path):
        return set()

    existing: Set[str] = set()
    with open(file_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key_value = row.get(key_field)
            if key_value:
                existing.add(key_value)
    return existing


def ensure_csv_headers(file_path: str, headers: List[str]) -> None:
    """CSV 파일이 존재하지 않는 경우 헤더를 작성한다.

    최초 실행 시에는 CSV 파일이 없을 수 있으므로, 파일이 없다면 헤더를
    생성하여 이후 append 방식으로 안전하게 데이터를 기록할 수 있도록 한다.
    """
    if os.path.exists(file_path):
        return
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()


def clean_text(value: Optional[str]) -> str:
    """텍스트에서 제로 폭 문자를 제거하고 공백을 정리한다."""
    if not value:
        return ""
    cleaned = ZERO_WIDTH_PATTERN.sub("", value)
    return cleaned.strip()


def chunked(iterable: List[str], size: int) -> Iterable[List[str]]:
    """리스트를 일정 크기로 분할한다.

    YouTube Data API는 한 번에 요청할 수 있는 ID 수에 제한이 있으므로
    (예: videos.list는 최대 50개), 이 헬퍼 함수를 통해 안전하게 분할한다.
    """
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def fetch_search_results(
    client: YouTubeAPIClient,
    category_id: str,
    per_category: int,
    region: str,
    published_after: Optional[str],
) -> List[str]:
    """search.list API를 사용해 영상 ID 목록을 수집한다.

    카테고리, 지역, 게시일 필터 등을 조합하여 최신 영상을 가져오며,
    `per_category` 한도를 만족할 때까지 페이지네이션을 반복한다.
    """
    collected: List[str] = []
    page_token: Optional[str] = None

    while len(collected) < per_category:
        params = {
            "part": "id",
            "type": "video",
            "order": "date",
            "videoCategoryId": category_id,
            "maxResults": "50",
            "regionCode": region,
        }
        if published_after:
            params["publishedAfter"] = published_after
        if page_token:
            params["pageToken"] = page_token

        data = client.request("search", params)
        items = data.get("items", [])
        for item in items:
            kind = item.get("id", {}).get("kind")
            if kind != "youtube#video":
                continue
            video_id = item.get("id", {}).get("videoId")
            if video_id:
                collected.append(video_id)
            if len(collected) >= per_category:
                break

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return collected


def fetch_video_details(client: YouTubeAPIClient, video_ids: List[str]) -> List[Dict]:
    """videos.list API를 호출해 영상의 상세 정보를 가져온다.

    조회된 영상 ID를 50개씩 묶어서 API 제한을 준수하며 요청하고,
    snippet, contentDetails, statistics 정보를 함께 받아온다.
    """
    details: List[Dict] = []
    for batch in chunked(video_ids, 50):
        params = {
            "part": "snippet,contentDetails,statistics",
            "id": ",".join(batch),
            "maxResults": "50",
        }
        data = client.request("videos", params)
        details.extend(data.get("items", []))
    return details


def fetch_channel_details(client: YouTubeAPIClient, channel_ids: Iterable[str]) -> List[Dict]:
    """channels.list API를 호출해 채널의 메타데이터를 수집한다.

    영상 수집 결과에서 새롭게 발견된 채널 ID만 추려 API 호출 횟수를
    최소화하고, 채널 이름/설명/통계 데이터를 저장한다.
    """
    details: List[Dict] = []
    channel_list = list(channel_ids)
    for batch in chunked(channel_list, 50):
        params = {
            "part": "snippet,statistics",
            "id": ",".join(batch),
            "maxResults": "50",
        }
        data = client.request("channels", params)
        details.extend(data.get("items", []))
    return details


def save_videos(file_path: str, videos: List[Dict]) -> None:
    """영상 정보를 CSV 파일에 저장한다.

    videos.csv에는 영상 ID, 카테고리, 채널, 게시일, 통계(JSON 문자열) 등
    핵심 메타데이터를 기록한다.
    """
    headers = ["videoId", "categoryId", "title", "channelId", "publishedAt", "statistics"]
    ensure_csv_headers(file_path, headers)

    with open(file_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        for video in videos:
            writer.writerow(video)


def save_channels(file_path: str, channels: List[Dict]) -> None:
    """채널 정보를 CSV 파일에 저장한다.

    채널 ID를 기준으로 중복을 제거한 뒤 새로 발견된 채널만 CSV에
    추가한다. 국가 정보나 통계 필드는 추후 분석을 위해 JSON 형태로 보관한다.
    """
    headers = [
        "channelId",
        "title",
        "description",
        "customUrl",
        "publishedAt",
        "country",
        "statistics",
    ]
    ensure_csv_headers(file_path, headers)

    with open(file_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        for channel in channels:
            writer.writerow(channel)


def crawl(args: argparse.Namespace) -> None:
    """크롤링 작업의 메인 로직.

    1. API 키 매니저 및 클라이언트를 초기화한다.
    2. 기존 videos.csv / channels.csv를 로드하여 중복을 필터링한다.
    3. 지정된 카테고리마다 search -> videos -> channels 순서로 데이터를
       수집하고 CSV로 저장한다.
    """
    keys = [k.strip() for k in args.api_keys.split(",") if k.strip()]
    key_manager = APIKeyManager(keys)
    client = YouTubeAPIClient(key_manager)

    videos_file = args.videos_file
    channels_file = args.channels_file

    existing_video_ids = read_existing_ids(videos_file, "videoId")
    existing_channel_ids = read_existing_ids(channels_file, "channelId")

    categories = [c.strip() for c in args.categories.split(",") if c.strip()]
    print(f"[안내] 총 {len(categories)}개의 카테고리를 대상으로 크롤링을 시작합니다.")

    new_videos: List[Dict] = []
    channel_ids_to_fetch: Set[str] = set()

    for category_id in categories:
        print(f"[안내] 카테고리 {category_id} 처리 중...")
        video_ids = fetch_search_results(
            client,
            category_id=category_id,
            per_category=args.per_category,
            region=args.region,
            published_after=args.published_after,
        )
        filtered_video_ids = [vid for vid in video_ids if vid not in existing_video_ids]
        if not filtered_video_ids:
            print(f"[안내] 카테고리 {category_id}에서 신규 영상이 없습니다.")
            continue

        details = fetch_video_details(client, filtered_video_ids)
        for item in details:
            video_id = item.get("id")
            if not video_id or video_id in existing_video_ids:
                continue
            snippet = item.get("snippet", {})
            title = clean_text(snippet.get("title", ""))
            channel_id = snippet.get("channelId", "")
            if channel_id:
                channel_ids_to_fetch.add(channel_id)

            video_record = {
                "videoId": video_id,
                "categoryId": category_id,
                "title": title,
                "channelId": channel_id,
                "publishedAt": snippet.get("publishedAt", ""),
                "statistics": json.dumps(item.get("statistics", {}), ensure_ascii=False),
            }
            new_videos.append(video_record)
            existing_video_ids.add(video_id)

    if new_videos:
        save_videos(videos_file, new_videos)
        print(f"[완료] {len(new_videos)}개의 신규 영상을 저장했습니다.")
    else:
        print("[안내] 저장할 신규 영상이 없습니다.")

    new_channel_ids = [cid for cid in channel_ids_to_fetch if cid not in existing_channel_ids]
    if not new_channel_ids:
        print("[안내] 저장할 신규 채널이 없습니다.")
        return

    channel_details = fetch_channel_details(client, new_channel_ids)
    new_channels: List[Dict] = []
    for item in channel_details:
        channel_id = item.get("id")
        if not channel_id or channel_id in existing_channel_ids:
            continue
        snippet = item.get("snippet", {})
        channel_record = {
            "channelId": channel_id,
            "title": clean_text(snippet.get("title", "")),
            "description": clean_text(snippet.get("description", "")),
            "customUrl": snippet.get("customUrl", ""),
            "publishedAt": snippet.get("publishedAt", ""),
            "country": snippet.get("country", ""),
            "statistics": json.dumps(item.get("statistics", {}), ensure_ascii=False),
        }
        new_channels.append(channel_record)
        existing_channel_ids.add(channel_id)

    if new_channels:
        save_channels(channels_file, new_channels)
        print(f"[완료] {len(new_channels)}개의 신규 채널을 저장했습니다.")
    else:
        print("[안내] 채널 API에서 데이터를 찾지 못했습니다.")


def export_titles(args: argparse.Namespace) -> None:
    """기존 videos.csv에서 영상 제목만 추출해 별도 CSV로 저장한다.

    NLP 분석이나 키워드 추출용으로 간단히 제목만 필요할 때 사용한다.
    제로 폭 문자를 제거하여 후처리 과정을 단순화한다.
    """
    source_file = args.videos_file
    output_file = args.export_titles

    if not os.path.exists(source_file):
        print(f"[오류] {source_file} 파일이 존재하지 않습니다.")
        return

    titles: List[Dict[str, str]] = []
    with open(source_file, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            title = clean_text(row.get("title", ""))
            if title:
                titles.append({"title": title})

    if not titles:
        print("[안내] 추출할 제목 데이터가 없습니다.")
        return

    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["title"])
        writer.writeheader()
        writer.writerows(titles)

    print(f"[완료] 총 {len(titles)}개의 제목을 {output_file} 파일로 내보냈습니다.")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """명령행 인자 파싱.

    argparse를 활용하여 크롤링 옵션, 출력 파일 경로, 제목 추출 기능 등을
    설정한다. `--export-titles`만 사용하는 경우 API 키가 없어도 실행할 수
    있도록 예외 처리를 포함한다.
    """
    parser = argparse.ArgumentParser(description="YouTube CSV 크롤러")
    parser.add_argument(
        "--api-keys",
        help="쉼표로 구분된 YouTube Data API 키 목록",
    )
    parser.add_argument(
        "--categories",
        default="",
        help="쉼표로 구분된 카테고리 ID 목록",
    )
    parser.add_argument(
        "--per-category",
        type=int,
        default=50,
        help="카테고리별 최대 수집 영상 수",
    )
    parser.add_argument(
        "--region",
        default="KR",
        help="검색에 사용할 지역 코드",
    )
    parser.add_argument(
        "--published-after",
        default=None,
        help="ISO8601 형식의 publishedAfter 파라미터",
    )
    parser.add_argument(
        "--videos-file",
        default="videos.csv",
        help="영상 메타데이터를 저장할 CSV 경로",
    )
    parser.add_argument(
        "--channels-file",
        default="channels.csv",
        help="채널 메타데이터를 저장할 CSV 경로",
    )
    parser.add_argument(
        "--export-titles",
        help="영상 제목만 별도 CSV로 내보내기",
    )

    args = parser.parse_args(argv)

    if args.export_titles and not args.api_keys:
        return args

    if not args.api_keys:
        parser.error("크롤링을 위해서는 --api-keys 인자가 필요합니다.")

    if not args.categories:
        parser.error("크롤링을 위해서는 --categories 인자가 필요합니다.")

    return args


def main(argv: Optional[List[str]] = None) -> None:
    """엔트리 포인트.

    명령행 인자를 파싱한 뒤, 제목 추출 모드인지 크롤링 모드인지에 따라
    알맞은 함수를 실행한다.
    """
    args = parse_args(argv)
    if args.export_titles:
        export_titles(args)
        return
    crawl(args)


if __name__ == "__main__":
    main()

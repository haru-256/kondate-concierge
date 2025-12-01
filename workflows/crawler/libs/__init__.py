from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

import httpx
from loguru import logger


class RobotGuard:
    """robots.txtの取得・解析を行い、クロール可否を判定するクラス。

    Webサイトのrobots.txtを非同期で取得・パースし、指定されたURLがクロール可能か判定します。
    また、Crawl-delayの設定やSitemapのURLリストを取得する機能も提供します。

    Attributes:
        base_url (str): 対象サイトのベースURL
        user_agent (str): クロールに使用するUser-Agent
        robots_txt_url (str): robots.txtの完全なURL
        parser (RobotFileParser): robots.txtをパースするパーサー
        loaded (bool): robots.txtがロード済みかどうかを示すフラグ
    """

    def __init__(self, base_url: str, user_agent: str = "*"):
        """RobotGuardインスタンスを初期化します。

        Args:
            base_url (str): 対象サイトのベースURL(例: "https://example.com")
            user_agent (str, optional): クロールに使用するUser-Agent名。Defaults to "*".
        """
        self.base_url = base_url
        self.user_agent = user_agent
        self.robots_txt_url = urljoin(base_url, "robots.txt")
        parser = RobotFileParser()
        parser.set_url(self.robots_txt_url)
        self.parser = parser
        self.loaded = False

    async def load(self, client: httpx.AsyncClient) -> None:
        """robots.txtを非同期で取得し、パーサーに読み込ませます。

        RobotFileParserは307リダイレクトに対応していないため、
        httpxで事前に取得してからパースします。
        レスポンスステータスに応じて以下の処理を行います:
        - 200: robots.txtをパースして読み込む
        - 404: 全てのURLのクロールを許可
        - その他: 安全のため全てのURLのクロールを拒否

        Args:
            client (httpx.AsyncClient): HTTPリクエストを送信するための非同期クライアント

        Raises:
            httpx.HTTPError: HTTP通信でエラーが発生した場合
        """
        # RobotFileParserは307リダイレクトに対応していないため、事前にhttpxで取得してからパースさせる
        resp = await client.get(self.robots_txt_url, timeout=10.0)
        if resp.status_code == 200:
            # テキストを行ごとに分割して標準パーサーに渡す
            lines = resp.text.splitlines()
            self.parser.parse(lines)
            logger.debug(f"Loaded robots.txt from {self.robots_txt_url}")
        elif resp.status_code == 404:
            # 404なら全許可とみなすのが一般的
            self.parser.parse([])
            logger.debug("robots.txt not found (Allow all)")
        else:
            # 403などの場合は安全側に倒して全拒否にするケースも多い
            self.parser.parse(["User-agent: *", "Disallow: /"])
            logger.debug(f"Failed to load robots.txt: {resp.status_code}")
        self.loaded = True

    def _check_loaded(self) -> None:
        """robots.txtがロード済みかを確認します。

        Raises:
            RuntimeError: robots.txtがまだロードされていない場合
        """
        if not self.loaded:
            logger.error("robots.txt not loaded yet.")
            raise RuntimeError("robots.txt not loaded yet.")

    def can_fetch(self, url: str) -> bool:
        """指定されたURLがクロール可能か判定します。

        robots.txtのルールに基づいて、現在のUser-Agentで
        指定されたURLにアクセス可能かを判定します。

        Args:
            url (str): クロール可否を判定するURL

        Returns:
            bool: クロール可能な場合はTrue、不可能な場合はFalse

        Raises:
            RuntimeError: robots.txtがまだロードされていない場合
        """
        self._check_loaded()
        return self.parser.can_fetch(self.user_agent, url)

    def get_crawl_delay(self) -> int | None:
        """Crawl-delay(クロール間隔の待機時間)の設定を取得します。

        robots.txtで指定されたCrawl-delay設定を取得します。
        設定がない場合はNoneを返します。

        Returns:
            float | None: Crawl-delay(秒数)。設定がない場合はNone

        Raises:
            RuntimeError: robots.txtがまだロードされていない場合
        """
        self._check_loaded()
        return self.parser.crawl_delay(self.user_agent)  # type: ignore

    def get_sitemaps(self) -> list[str]:
        """robots.txtで指定されたSitemapのURLリストを取得します。

        robots.txtに記載されているSitemap行から、
        全てのSitemapのURLを抽出して返します。

        Returns:
            list[str]: SitemapのURLリスト。Sitemapが存在しない場合は空リスト

        Raises:
            RuntimeError: robots.txtがまだロードされていない場合
        """
        self._check_loaded()
        return self.parser.sitemaps  # type: ignore

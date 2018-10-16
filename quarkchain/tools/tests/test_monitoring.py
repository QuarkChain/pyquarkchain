import unittest
from quarkchain.tools.monitoring import crawl_recursive, crawl_bfs


class TestCrawl(unittest.TestCase):
    def test_crawl(self):
        cache = {}
        res = {}
        # crawl_recursive(cache, "54.152.237.112", 38291, 38291 + 200, ip_lookup={})
        # res = crawl_bfs("54.152.237.112", 38291, 38291 + 200)
        dfs = {k: set(v) for k, v in cache.items()}
        bfs = {k: set(v) for k, v in res.items()}
        self.assertEqual(dfs, bfs)

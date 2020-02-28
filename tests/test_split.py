from unittest import TestCase, main as run_tests


from etltools.streamtools import split


class TestSplitStream(TestCase):
    def setUp(self):
        self.iterable = [
            (i, 2*i) for i in range(5)
        ]

    def test_1(self):
        it1, it2 = split(lambda t: ((t[0], ), (t[1], ), ), self.iterable)
        self.assertListEqual(
            list((i, ) for i in range(5)),
            list(it1)
        )

        self.assertListEqual(
            list((2*i, ) for i in range(5)),
            list(it2)
        )


if __name__ == '__main__':
    run_tests(verbosity=2)

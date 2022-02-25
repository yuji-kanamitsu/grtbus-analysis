from chardet.universaldetector import UniversalDetector

# 文字コード判定
def get_file_encoding(file_path) :
    detector = UniversalDetector()
    with open(file_path, mode="rb") as f:
        for binary in f:
            detector.feed(binary)
            if detector.done:
                break
    detector.close()
    return detector.result["encoding"]
import torch
print(torch.__version__)  # 应该输出 PyTorch 版本，如 2.7.1
print(torch.cuda.is_available())  # 应该返回 True（如果 GPU 可用）
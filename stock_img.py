import time

from PIL import Image, ImageDraw, ImageFont
import pandas as pd

# 微博-长夜几安的二筛股票池
codes = """605208,605303,605339,605358,688018,688069,688072,688114,688139,688160,688169,688208,688222,688256,688257,688268,688322,688337,688360,688361,688377,688416,688531,688548,688557,688585,688593,688608,688698,688708,000949,003021,300005,002983,002989,002997,300162,300199,300222,300233,300303,300328,300373,300394,300403,300421,301075,301076,301155,301171,300502,300503,300560,300648,300652,300660,300689,300718,300720,300727,300733,300757,300783,300801,300812,300946,300429,300436,300456,300475,300981,300984,301013,301019,301023,301199,301383,301446,301606,605128,605167,603897,603728,603730,603739,000620,000670,000678,603699,603508,603579,603598,603324,603332,603373,000839,000863,000880,688213,603226,603233,603266,603163,603179,603197,001326,001332,603067,603015,603040,603057,002050,601918,601985,002119,002124,002127,002181,002191,002194,002196,002249,002293,601100,601177,600847,002407,600415,600222,600207,002637,002871,002906"""

# 分割成列表
code_list = codes.split(',')
print(len(code_list))
# 设置每行显示数量
cols = 8
# 计算行数
rows = (len(code_list) + cols - 1) // cols

# 图片参数
font_size = 28
cell_width = 140
cell_height = 50
padding = 20

img_width = cols * cell_width + 2 * padding
img_height = rows * cell_height + 2 * padding

# 创建白底图片
img = Image.new('RGB', (img_width, img_height), 'white')
draw = ImageDraw.Draw(img)

# 尝试加载中文字体，若无则使用默认字体
try:
    font = ImageFont.truetype("simhei.ttf", font_size)
except:
    font = ImageFont.load_default()

# 绘制股票代码
for i, code in enumerate(code_list):
    row = i // cols
    col = i % cols
    x = padding + col * cell_width
    y = padding + row * cell_height
    draw.text((x, y), code, fill='black', font=font)

# 保存并检查大小
img_file = f'weibo_stocks_{time.strftime("%Y%m%d")}.png'
img.save(img_file, 'PNG', optimize=True)
import os
size_kb = os.path.getsize(img_file) / 1024
print(f"图片已生成：stocks_clear.png，大小约 {size_kb:.1f} KB，清晰度较高")

file_name = 'result_20260515_1811.csv'
df = pd.read_csv(file_name, encoding='utf-8-sig')
columns = df.columns.values.tolist()  ### 获取excel 表头 ，第一行
data_list = []
for idx, row in df.iterrows():
    temp = {}
    for column in columns:
        temp[column] = row[column]
    data_list.append(temp)
# print(data_list)

# 查询长夜几安每周的2筛股票池和模型选股的重叠
for data in data_list:
    dcode = data['股票代码'].split('.')[1]
    if dcode in code_list and ('买点2' in data['信号类型'] or '买点1-W2' in data['信号类型']):
        print(dcode, data)
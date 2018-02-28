# -*- mode: python -*-

block_cipher = None


a = Analysis(['hilltop_wq_export_weekly.py'],
             pathex=['E:\\ecan\\git\\HydroPandas\\users\\MK\\tsdata_exports\\hilltop_wq_export'],
             binaries=[],
             datas=[],
             hiddenimports=['win32timezone'],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)
			 
def get_pandas_path():
	import pandas
	pandas_path = pandas.__path__[0]
	return pandas_path

dict_tree = Tree(get_pandas_path(), prefix='pandas', excludes=["*.pyc"])
a.datas += dict_tree
a.binaries = filter(lambda x: 'pandas' not in x[0], a.binaries)

pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='hilltop_wq_export_weekly',
          debug=False,
          strip=False,
          upx=True,
          runtime_tmpdir=None,
          console=True )

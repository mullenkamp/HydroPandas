# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 14:59:13 2018

@author: michaelek
"""

from kivy.app import App
from kivy.uix.gridlayout import GridLayout
from kivy.uix.textinput import TextInput


class Screen(GridLayout):
    def __init__(self, **kwargs):
        super(Screen, self).__init__(**kwargs)
        self.input = TextInput(multiline=False)
        self.add_widget(self.input)
        self.input.bind(on_text_validate=self.print_input)
    def print_input(self, value):
        print(value.text)


class MyApp(App):

    def build(self):
        return Screen()


if __name__ == '__main__':
    MyApp().run()

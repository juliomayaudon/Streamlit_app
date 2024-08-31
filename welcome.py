
import streamlit as st
import requests
from streamlit_lottie import st_lottie
from PIL import Image


#Funcion para nuestra animacion
def load_lottieurl(url):
  r = requests.get(url)
  if r.status_code != 200:
    return None
  return r.json()

lottie_coding = load_lottieurl("https://lottie.host/8f07cd91-d92f-42b7-8321-eecbcb3efd07/JBXLxTmpj6.json")

with st.container():
  st.subheader("Welcome to the ABM APP")
  st.title("About the application")
  st.write("Welcome to the app under development for the ABM team. The goal of this page is to help the team members to reduce lead times and standardize processes.")
  st.write("[More info >](https://www.loom.com/looms/videos)")

with st.container():
  st.write("---")
  left_column, right_column = st.columns(2)
  with left_column:
    st.header("The Objective")
    st.write(
      """
        The idea of this project is to implement technologies developed by the Kalungi team using techniques such as a data retrieved model  using artificial intelligence in order to develop a tool that can be used both for internal use and as a product that can be offered to a customer segment. 
      """
    )
    st.write("[more info >](https://docs.google.com/presentation/d/1OZUBRlpVaL9_V9NNy9B8Afpfw7qicZkn3OHA0gaccck/edit)")
  with right_column:
    st_lottie(lottie_coding, height=300, key="coding")

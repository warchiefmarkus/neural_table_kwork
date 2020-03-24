# -*- encoding: utf-8 -*-
import os, logging 
# Flask modules
from flask               import render_template, request, url_for, redirect, send_from_directory
from werkzeug.exceptions import HTTPException, NotFound, abort
# App modules
from app        import app, db, bc
from app.models import User
from app.forms  import LoginForm, RegisterForm
# App main route + generic routing
@app.route('/')
def index():
    return render_template('layouts/default.html',
                                content=render_template('pages/index.html'))

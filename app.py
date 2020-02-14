from flask import Flask, jsonify,request
from flask_cors import CORS, cross_origin
import statement1dbope as st1db
from flask_pymongo import PyMongo
from flask_jwt_extended import (
    JWTManager, jwt_required, create_access_token,
    get_jwt_identity, get_jwt_claims

)
app = Flask(__name__)
CORS(app)


app.config["MONGO_URI"] = "mongodb://localhost:27017/dhi_analytics"


mongo = PyMongo(app)
# Setup the Flask-JWT-Extended extension

app.config['JWT_SECRET_KEY'] = 'super-secret' 
jwt = JWTManager(app)


class UserObject:
    def __init__(self, username, roles,emlpoyeeGivenId,usn):
        self.username = username
        self.roles = roles
        self.emlpoyeeGivenId = emlpoyeeGivenId
        self.usn = usn
 

@jwt.user_claims_loader
def add_claims_to_access_token(user):
    return {'roles': user.roles,"emlpoyeeGivenId":user.emlpoyeeGivenId,"usn":user.usn}

@jwt.user_identity_loader
def user_identity_lookup(user):
    return user.username

# Provide a method to create access tokens. The create_access_token()
# function is used to actually generate the token, and you can return
# it to the caller however you choose.
@app.route('/login', methods=['POST'])
def login():
    emlpoyeeGivenId = ''
    usn = ''
    if not request.is_json:
        return jsonify({"msg": "Missing JSON in request"}), 400
    username = request.json.get('username', None)
    if not username:
        return jsonify({"msg": "Missing username parameter"}), 400
    user = mongo.db.dhi_user.find_one({'email': username})
    if not user:
        return jsonify({"msg": "Bad username or password"}), 401
    roles = [ x['roleName'] for x in user['roles']]
    if 'employeeGivenId' in user:
        emlpoyeeGivenId = user["employeeGivenId"]
    if 'usn' in user:
        usn = user["usn"]
    user = UserObject(username=user["email"], roles=roles,emlpoyeeGivenId = emlpoyeeGivenId,usn = usn)
    # Identity can be any data that is json serializable
    access_token = create_access_token(identity=user,expires_delta=False)
    return jsonify(access_token=access_token), 200

@app.route('/message')
def message():
    return {"message":"Check you luck"}



# Protect a view with jwt_required, which requires a valid access token
# in the request to access.


@app.route('/user', methods=['GET'])
@jwt_required
def protected():
    # Access the identity of the current user with get_jwt_identity
    ret = {
            'user': get_jwt_identity(),  
            'roles': get_jwt_claims()['roles'] ,
            'employeeGivenId':get_jwt_claims()['emlpoyeeGivenId'],
            'usn':get_jwt_claims()['usn']
          }
        
    return jsonify(ret), 200


@app.route('/academicyear')
def getacademicYear():
    year = st1db.getacademicYear()
    return jsonify({'acdemicYear':year})

@app.route('/termNumber')
def get_term_numbers():
    terms = st1db.get_term_numbers()
    return jsonify({'term_numbers':terms})


@app.route("/attendancedetails/<string:usn>/<string:academicYear>/<termNumber>")
# view all the documents present in db
def get_attendance_details(usn, academicYear, termNumber):
    termNumber = list(termNumber.split(','))
    attendance_percent = st1db.get_details(usn, academicYear, termNumber)
    return jsonify({"attendance_percent": attendance_percent})

@app.route('/subjectavgdetails/<string:facultyname>/<year>/<term>')
def get_subject_avg_attendance(facultyName, year, term):
    term = list(term.split(','))
    attendance_percent = st1db.get_details(facultyName, year, term)
    return jsonify({"attendance_percent":attendance_percent})

@app.route('/iadetails/<string:usn>/<courseCode>/<section>/<termNumber>/<deptId>/<year>') 
def get_ia_detail(usn,courseCode,section,termNumber,deptId,year):
    ia_details = st1db.get_iadetails(usn,courseCode,section,termNumber,deptId,year)
    return jsonify({"ia_details":ia_details})

@app.route('/getCourseAttendance/<course>/<usn>')
def courseAttendance(course,usn):
    res = st1db.getCourseAttendance(course,usn)
    return jsonify({"res":res})

# @app.route('/getUsn/<email>')
# def getUsn(email):
#     usn = st1db.getUsnByEmail(email)
#     return jsonify({"usn":usn})

# @app.route('/getFacultyId/<email>')
# def getFacultyid(email):
#     eid = st1db.getFacultyId(email)
#     return jsonify({"res":eid})

# @app.route('/getFacultyAttendance/<eid>/<academic>/<term>')
# def getFacultyAttendance(eid,academic,term):
#     attend = st1db.getFacultyAttendance(eid,academic,term)
#     return jsonify({"res":attend})
@app.route('/faculties/<string:dept>')
def get_faculty(dept):
    facluties = st1db.get_faculty(dept)
    return jsonify({"faculties":facluties})

@app.route('/alldepartment')
def get_departments():
    department = st1db.get_all_departments()
    return jsonify({"department":department})

# @app.route()
# def get_branchwise_faculties(dept):
#     faculties = st1db.get_branchwise_faculty(dept)
#     return jsonify({"faculties":faculties})
@app.route('/deptfaculties/<string:dept>')
def get_faculty_by_dept(dept):
    department = st1db.get_faculty_by_dept(dept)
    return jsonify({"department":department})

@app.route('/facsubattendance/<string:eid>/<year>')
def fac_sub_avg_attendance(eid,year):
    avg_per=st1db.faculty_subjectwise_attendance(eid,year)
    return jsonify({"avg_per":avg_per})

@app.route('/get-faculty-by-dept/<dept>')
def get_faculties_by_dept(dept):
    faculty = st1db.get_faculties_by_dept(dept)
    return jsonify({"faculty" : faculty})

@app.route('/get-dept-name')
def get_dept_names():
    dept = st1db.get_all_depts()
    return jsonify({"dept" : dept})

@app.route("/empiadetails/<string:eid>/<string:courseCode>/<string:deptId>/<string:academicYear>")
# view all the documents present in db
def get_emp_ia_details(eid, courseCode, deptId, academicYear):
#termNumber = list(termNumber.split(','))
    ia_per = st1db.get_emp_ia_details(eid,courseCode,deptId,academicYear)
    return jsonify({"ia_per": ia_per})

# Praneeth Started
@app.route('/emps/<dept>')
def getEmpByDept(dept):
    emps = st1db.get_faculties_by_dept(dept)
    return jsonify({"faculties":emps})

@app.route('/get-selected-fac-details/<empid>/<term>')
def get_fac_details(empid , term):
    res = st1db.get_fac_wise_details(empid,term)
    return jsonify({"fac":res})

# @app.route('/get-attendence-for-course/<sub>/<usn>')
# def get_attendence_for_sub(sub,usn):
#     attendence = st1db.get_attendence_for_course(sub,usn)
#     return jsonify({"res":attendence})

@app.route('/emp/ia/total/<empid>/<term>/<sem>')
def getEmpIaTotalMarks(empid,term,sem):
    iamarks = st1db.get_emp_subjects(empid,term,sem)
    return jsonify({"iamarks":iamarks})

# @app.route('/emp/ia/total/<empid>/<term>/<sem>')faculty_ia_details(fac_id,year,courseCode,section,deptId,term)
# def getEmpIaTotalMarks(empid,term,sem):
#     iamarks = st1db. get_emp_sub_attendence(empid,course)
#     return jsonify({"iamarks":iamarks})

@app.route('/get-select-details/<empid>/<term>/<sem>')
def get_select_details(empid,term,sem):
    iamarks = st1db.get_emp_subjects(empid,term,sem)
    return jsonify({"iamarks":iamarks})

@app.route('/get-faculty-ia-details/<fac_id>/<year>/<courseCode>/<section>/<deptId>/<terms>')
def get_faculty_ia_details(fac_id,year,courseCode,section,deptId,terms):
    iamark = st1db.faculty_ia_details(fac_id,year,courseCode,section,deptId,terms)
    return jsonify({"iamark":iamark})

@app.route('/depts')
def getAllDept():
    depts = st1db.get_all_depts1()
    return jsonify({"depts":depts})

if __name__ == "__main__":
    app.run(debug=True)


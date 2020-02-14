from pymongo import MongoClient
from pprint import pprint
from operator import itemgetter
import bson
import re
db = MongoClient()
mydb = db.dhi_analytics
dhi_internal = mydb['dhi_internal']
dhi_term_details = mydb['dhi_term_detail']
dhi_student_attendance = mydb['dhi_student_attendance']
dhi_user = mydb['dhi_user']


def getacademicYear():
    academicYear = dhi_internal.aggregate([{"$group": {"_id": "null",
                                                       "academicYear": {"$addToSet": "$academicYear"}}}, {"$project": {"academicYear": "$academicYear", "_id": 0}}])
    for year in academicYear:
        year = year['academicYear']
    return year


def get_term_numbers():
    terms_numbers = dhi_term_details.aggregate([
        {"$unwind": "$academicCalendar"},
        {"$group": {"_id": "null", "termNumber": {
            "$addToSet": "$academicCalendar.termNumber"}}},
        {"$project": {"_id": 0}}
    ])
    for term in terms_numbers:
        terms = term['termNumber']
    terms.sort()
    return terms


def get_all_departments():
    departments = dhi_internal.aggregate([
        {"$unwind": "$departments"},
        {"$group": {"_id": "null", "departments": {
            "$addToSet": "$departments.deptName"}}},
        {"$project": {"_id": 0}}
    ])
    for department in departments:
        department = department['departments']

    return department


def get_ia_details(usn, courseCode, section, termNumber, deptId, year):
    ia_percent = 0
    avg_ia_score = 0

    ia_details = [x for x in dhi_internal.aggregate([
        {
            '$unwind': '$studentScores'
        },
        {'$unwind': '$departments'},
        {'$unwind': '$studentScores.evaluationParameterScore'},
        {
            '$match':
            {
                'studentScores.usn': usn,
                'academicYear': year,
                'courseCode': courseCode,
                'studentScores.section': section,
                'departments.deptId': deptId,
                'studentScores.termNumber': termNumber
            }

        },

        {
            '$group':
            {
                '_id': '$iaNumber',
                "maxMarks": {"$addToSet": "$studentScores.evaluationParameterScore.maxMarks"},
                "iaNumber": {"$addToSet": "$iaNumber"},
                "obtainedMarks": {"$addToSet": "$studentScores.totalScore"},
                "startTime": {"$addToSet": "$startTime"}
            }
        },
        {'$unwind': '$maxMarks'},
        {'$unwind': '$iaNumber'},
        {'$unwind': '$startTime'},
        {'$unwind': '$obtainedMarks'},
        {
            "$project":
                {
                    "_id": 0,
                    "maxMarks": "$maxMarks",
                    "obtainedMarks": "$obtainedMarks",
                    "startTime": "$startTime",
                    "iaNumber": "$iaNumber"
                }
        }

    ])]
    for x in ia_details:
        try:
            ia_percent = (x['obtainedMarks']/x['maxMarks'])*100
            ia_percent = round(ia_percent, 2)
            x['ia_percent'] = ia_percent
            avg_ia_score = avg_ia_score + ia_percent
        except ZeroDivisionError:
            avg_ia_score = 0

    try:
        avg_ia_score = avg_ia_score/len(ia_details)
        avg_ia_score = round(avg_ia_score, 2)
        return ia_details, avg_ia_score
    except ZeroDivisionError:
        return ia_details, 0


def get_avg_attendance(usn, courseCode, section, termNumber, deptId, year):

    for attedance_details in dhi_student_attendance.aggregate([
        {'$unwind': '$departments'},
        {'$unwind': '$students'},


        {
            '$match':
            {
                'academicYear': year,
                'students.usn': usn,
                'courseCode': courseCode,
                'students.deptId': deptId,
                'students.section': section,
                'students.termNumber': termNumber
            }
        },

        {
            '$project':
            {
                '_id': 0,
                'totalNumberOfClasses': '$students.totalNumberOfClasses',
                'totalPresent': '$students.presentCount',
                'totalAbsent': '$students.absentCount'
            }
        }

    ]):
        attendance_per = (
            attedance_details['totalPresent']/attedance_details['totalNumberOfClasses'])*100
        attendance_per = round(attendance_per, 2)
        attendance = {"attedance_details": attedance_details,
                      "attendance_per": attendance_per}
        return attendance


def get_iadate_wise_attendance(usn, courseCode, section, termNumber, deptId, year, iadate, iaNumber):
    present_details = []
    present = []
    absent = []
    perc_of_present = 0
    perc_of_absent = 0
    for x in dhi_student_attendance.aggregate([
        {'$unwind': '$departments'},
        {'$unwind': '$students'},
        {
            '$match':
            {
                'academicYear': year,
                'students.usn': usn,
                'courseCode': courseCode,
                'students.deptId': deptId,
                'students.section': section,
                'students.termNumber': termNumber
            }
        },
        {'$unwind': '$students.studentAttendance'},
        {
            '$match':
            {
                "students.studentAttendance.date": {"$lte": iadate}
            }
        },
        {
            '$project':
            {
                "_id": 0,
                "date": "$students.studentAttendance.date",
                        "present": "$students.studentAttendance.present"
            }
        }

    ]):
        present_details.append(x['present'])
        if x['present'] == True:
            present.append(x['present'])
        if x['present'] == False:
            absent.append(x['present'])
    try:
        perc_of_present = (len(present)/len(present_details))*100
        perc_of_present = round(perc_of_present, 2)
        perc_of_absent = (len(absent)/len(present_details))*100
        perc_of_absent = round(perc_of_absent, 2)
    except:
        perc_of_present = 0
        perc_of_absent = 0

    return perc_of_present, perc_of_absent


def get_details(usn, year, terms):
    final_attendance = []

    for x in dhi_internal.aggregate([
        {'$unwind': '$studentScores'},
        {'$unwind': '$departments'},
        {
            '$match':
            {
            'studentScores.usn': usn,
            'academicYear': year,
            'departments.termNumber': {'$in': terms}
            }
        },
        {
            '$group':
            {
                '_id':
                {
                    'courseCode': '$courseCode',
                    'courseName': '$courseName',
                    'section': '$studentScores.section',
                    'termNumber': '$studentScores.termNumber',
                    'deptId': '$departments.deptId'
                }
            }
        }
    ]):
        details = {}
        ia_details, avg_ia_score = get_ia_details(usn, x['_id']['courseCode'], x["_id"]
                                                  ["section"], x["_id"]["termNumber"], x["_id"]["deptId"], year)
        attedance_total_avg_details = get_avg_attendance(usn, x['_id']['courseCode'], x["_id"]
                                                         ["section"], x["_id"]["termNumber"], x["_id"]["deptId"], year)
        for ia_detail in ia_details:
            try:
                ia_detail['perc_of_present'], ia_detail['perc_of_absent'] = get_iadate_wise_attendance(usn, x['_id']['courseCode'], x["_id"]
                                                                                                       ["section"], x["_id"]["termNumber"], x["_id"]["deptId"], year, ia_detail['startTime'], ia_detail['iaNumber'])
            except KeyError:
                ia_detail['perc_of_present'] = 0
                ia_detail['perc_of_absent'] = 0
        details['total_avg'] = {}
        details['attendance_per'] = 0
        details['courseCode'] = x['_id']['courseCode']
        details['courseName'] = x['_id']['courseName']
        details['section'] = x['_id']['section']
        details['termNumber'] = x['_id']['termNumber']
        details['deptId'] = x['_id']['deptId']
        details['ia_attendance_%'] = ia_details
        details['avg_ia_score'] = avg_ia_score
        if attedance_total_avg_details != None:
            details['total_avg'] = attedance_total_avg_details['attedance_details']
            details['attendance_per'] = attedance_total_avg_details['attendance_per']
        final_attendance.append(details)
    return final_attendance


def subject_avg_attendance(facultyName, year, term):
    for attendance_details in dhi_student_attendance.aggregate([
        {'$match': {'academicYear': year, 'students.termNumber': term}},
        {'$unwind': {'path': '$faculties'}},
        {'$unwind': {'path': '$faculties.facultyName'}},
        {'$match': {'faculties.facultyName': facultyName}},
        {'$group': {'_id': {'avg': {'$avg': '$students.percentage'},
                            'faculty': '$faculties.facultyName', 'course': '$courseName'}}},
        {'$project': {'faculty': '$_id.faculty', 'course': '$_id.course',
                      '_id': 0, 'attendance_avg': '$_id.avg'}}
    ]):
        attendance_per = round(attendance_details, 2)
        attendance = {"attedance_details": attendance_details,
                      "attendance_per": attendance_per}
        return attendance


def get_iadetails(usn, courseCode, section, termNumber, deptId, year):

    ia_details = [x for x in dhi_internal.aggregate([
        {
            '$unwind': '$studentScores'
        },
        {'$unwind': '$departments'},
        {'$unwind': '$studentScores.evaluationParameterScore'},
        {
            '$match':
            {
                'studentScores.usn': usn,
                'academicYear': year,
                'courseCode': courseCode,
                'studentScores.section': section,
                'departments.deptId': deptId,
                'studentScores.termNumber': termNumber
            }

        },

        {
            '$group':
            {
                '_id': '$iaNumber',
                "maxMarks": {"$addToSet": "$studentScores.evaluationParameterScore.maxMarks"},
                "iaNumber": {"$addToSet": "$iaNumber"},
                "obtainedMarks": {"$addToSet": "$studentScores.totalScore"},
                "startTime": {"$addToSet": "$startTime"}
            }
        },
        {'$unwind': '$maxMarks'},
        {'$unwind': '$iaNumber'},
        {'$unwind': '$startTime'},
        {'$unwind': '$obtainedMarks'},
        {
            "$project":
                {
                    "_id": 0,
                    "maxMarks": "$maxMarks",
                    "obtainedMarks": "$obtainedMarks",
                    "startTime": "$startTime",
                    "iaNumber": "$iaNumber"
                }
        }

    ])]
    # pprint(ia_details)
    li = []

    for i in ia_details:
        new = {}
        ia_percent = (i['obtainedMarks']/i['maxMarks'])*100
        new['iaNumber'] = i['iaNumber']
        new['maxMarks'] = i['maxMarks']
        new['obtainedMarks'] = i['obtainedMarks']
        new['ia_percent'] = ia_percent
        # print(new)
        li.append(new)
  
    return li
    # pprint(new)


def getCourseAttendance(course, usn):
    collection = mydb.dhi_student_attendance
    res = collection.aggregate([
        {"$match": {"courseName": course}},
        {"$unwind": "$students"},
        {"$match": {"students.usn": usn}},
        {"$project": {"total": "$students.totalNumberOfClasses",
                      "present": "$students.presentCount", "_id": 0}}

    ])
    arr = []
    for x in res:
        arr.append(x)
    return arr.pop()

def get_faculty(dept):
    pattern = re.compile(f'^{dept}')
    regex = bson.regex.Regex.from_native(pattern)
    regex.flags ^= re.UNICODE
    faculties = dhi_user.aggregate([
        {"$match": {"roles.roleName": "FACULTY", "employeeGivenId": {"$regex": regex}}},
        {"$sort": {"name": 1}},
        {"$project": {"employeeGivenId": 1, "name": 1, "_id": 0}}
    ])
    res = []
    for x in faculties:
        res.append(x)
    return res


def get_faculty_by_dept(dept):
    pattern = re.compile(f'^{dept}')
    regex = bson.regex.Regex.from_native(pattern)
    regex.flags ^= re.UNICODE
    faculties = dhi_user.aggregate([
        {"$match": {"roles.roleName": "FACULTY", "employeeGivenId": {"$regex": regex}}},
        {"$sort": {"name": 1}},
        {"$project": {"employeeGivenId": 1, "name": 1, "_id": 0}}
    ])
    res = []
    for x in faculties:
        res.append(x)
    return res


#     return faculty
# #get_branchwise_faculty('IS')


# get student's ia scores


def get_emp_ia_details(eid, courseCode, deptId, year):
    ia_percent = 0
    avg_ia_score = 0
    ia_details = [x for x in dhi_internal.aggregate([
    {
        '$unwind': '$studentScores'
    }, {'$unwind': '$faculties'},
    {'$unwind': '$departments'},
    {'$unwind': '$studentScores.evaluationParameterScore'},
    {
        '$match':
        {
            'faculties.facultyGivenId': eid,
            'academicYear': year,
            'courseCode': courseCode,
            'departments.deptId': deptId
        }
    },
    {
        '$group':
        {
            '_id': '$iaNumber',
            "maxMarks": {"$addToSet": "$studentScores.evaluationParameterScore.maxMarks"},
            "iaNumber": {"$addToSet": "$iaNumber"},
            "obtainedMarks": {"$addToSet": "$studentScores.totalScore"},
            "startTime": {"$addToSet": "$startTime"}
        }
    },
    {'$unwind': '$maxMarks'},
    {'$unwind': '$iaNumber'},
    {'$unwind': '$startTime'},
    {'$unwind': '$obtainedMarks'},
    {
        "$project":
        {
            "_id": 0,
            "maxMarks": "$maxMarks",
            "obtainedMarks": "$obtainedMarks",
            "startTime": "$startTime",
            "iaNumber": "$iaNumber"
        }
    }
    ])]
    for i in ia_details:
        ia_percent = (i['obtainedMarks']/i['maxMarks'])*100
        avg_ia_score = avg_ia_score + ia_percent
        avg_ia_score = avg_ia_score/len(ia_details)
    return ia_details, avg_ia_score


def faculty_subjectwise_attendance(eid, year):

    avg_attendance = dhi_student_attendance.aggregate([
        {"$unwind": "$faculties"},
        {"$unwind": "$students"},
        {"$unwind": "$courseName"},
        {"$match": {"faculties.employeeGivenId": eid, "academicYear": year}},
        {"$group": {"_id": "$courseName", "totalPercentage": {
            "$avg": "$students.percentage"}, "peopleCount": {"$sum": 1}}},
        {"$project": {"course": "$_id", "totalPercentage": 1, "peopleCount": 1, "_id": 0}}

    ])
    total = []
    for total_per in avg_attendance:
        new = {}
        new['course'] = total_per['course']
        new['avg_per'] = total_per['totalPercentage']
        total.append(new)
    return total


def get_faculties_by_dept(dept):
    collection = mydb.dhi_user
    pattern = re.compile(f'^{dept}')
    regex = bson.regex.Regex.from_native(pattern)
    regex.flags ^= re.UNICODE
    faculties = collection.aggregate([
        {"$match": {"roles.roleName": "FACULTY", "employeeGivenId": {"$regex": regex}}},
        {"$project": {"employeeGivenId": 1, "name": 1, "_id": 0}}
    ])
    res = [f for f in faculties]

    return res


def get_all_depts():
    collection = mydb.dhi_user
    depts = collection.aggregate([
        {"$match": {"roles.roleName": "FACULTY"}},
        {"$project": {"_id": 0, "employeeGivenId": 1}}
    ])
    res = []
    for d in depts:
        if "employeeGivenId" in d:
            res.append(d["employeeGivenId"])
    # print(len(res))
    dept = []
    for d in res:
        name = re.findall('([a-zA-Z]*).*', d)
        if name[0].upper() not in dept:
            dept.append(name[0].upper())
    dept.remove('ADM')
    dept.remove('EC')
    return dept
    # print(dept)



# def get_faculties_by_dept(dept):
#     collection = mydb.dhi_user
#     pattern = re.compile(f'^{dept}')
#     regex = bson.regex.Regex.from_native(pattern)
#     regex.flags ^= re.UNICODE 
#     faculties = collection.aggregate([
#         {"$match":{"roles.roleName":"FACULTY","employeeGivenId":{"$regex":regex}}},
#         {"$project":{"employeeGivenId":1,"name":1,"_id":0}},
#         {"$sort":{"name":1}}
#     ])
#     res = [f for f in faculties]
#     return res

def get_fac_wise_details(empid , term):
    collection = mydb.dhi_student_attendance
    attendance = collection.aggregate([
          {"$match":{"faculties.employeeGivenId":empid,"departments.termNumber":term}},
          {"$unwind":"$students"},
          {"$unwind":"$courseCode"},
          {"$group":{"_id":"$courseCode","totalPercentage":{"$avg":"$students.percentage"},"totalClasses":{"$sum":"$student.totalNumberOfClasses"}}},
          {"$project" : {"courseid":"$_id","totalPercentage":1,"totalClasses":1,"_id":0}}
          ])
        
    att = [r for r in attendance]

    # res = []
    # for mark in att:
    #     if mark['totalPercentage'] != 0:
    #         mark['totalPercentage'] = mark['totalPercentage']
    #     else:
    #         mark['totalPercentage'] = 0
    #     mark['totalClasses'] = mark['totalClasses']
    #     res.append(mark)
    return att


def get_emp_sub_attendence(empid,course):
    collection = mydb.dhi_student_attendance
    attendance = collection.aggregate([
          {"$match":{"faculties.employeeGivenId":empid,"courseCode":course}},
          {"$unwind":"$students"},
          {"$unwind":"$courseCode"},
          {"$group":{"_id":"$courseCode","totalPercentage":{"$avg":"$students.percentage"},"totalClasses":{"$sum":"$student.totalNumberOfClasses"}}},
          {"$project" : {"courseid":"$_id","totalPercentage":1,"totalClasses":1,"_id":0}}
          ])
        
    att = [r for r in attendance]
    print(att)
    return att
# m=get_emp_sub_attendence("CIV598","15CV34")
# print(m)


def get_emp_subjects(empid,term,sem):
    collection = mydb.dhi_internal
    marks = collection.aggregate([
    {"$match":{"faculties.facultyGivenId":empid,"academicYear":term,"departments.termNumber":sem}},
    {"$unwind":"$departments"},
    {"$unwind":"$studentScores"},
    {"$match":{"studentScores.totalScore":{"$gt":0}}},
    {"$group":{"_id":"$courseCode","totalMarks":{"$avg":"$studentScores.totalScore"},"maxMarks":{"$avg":"$evaluationParameters.collegeMaxMarks"},
    "deptId":{"$first":"$departments.deptId"},"termNumber":{"$first":"$departments.termNumber"},"section":{"$first":"$departments.section"},
    "courseCode":{"$first":"$courseCode"},"courseName":{"$first":"$courseName"}}},
    {"$project":{"_id":0}}
    ])

    res = []
    for mark in marks:
        pprint(mark)
        attendence = get_emp_sub_attendence(empid,mark['courseCode'])
        if mark['maxMarks'] != 0:
            mark['iaPercentage'] = 100 * mark['totalMarks'] / mark['maxMarks'] 
        else:
            mark['iaPercentage'] = 0
        if len(attendence) != 0:
            mark['attendence'] = attendence[0]['totalPercentage']
        else:
            mark['attendence'] = 0
        
        res.append(mark)
    return res
# n=get_emp_subjects("CIV598","2017-18","3")
# print(n)


# get_emp_subjects("ISE228","2017-18","8")

def get_all_depts1():
    collection = mydb.dhi_user
    depts = collection.aggregate([
        {"$match":{"roles.roleName":"FACULTY"}},
        {"$project":{"_id":0,"employeeGivenId":1}}
    ])
    res = []
    for d in depts:
        if "employeeGivenId" in d:
            res.append(d["employeeGivenId"])
    dept = []
    for d in res:
        name = re.findall('([a-zA-Z]*).*',d)
        if name[0].upper() not in dept:
            dept.append(name[0].upper())
    dept.remove('ADM')
    dept.remove('EC')
    return dept

def faculty_ia_details(fac_id,year,courseCode,section,deptId,terms):
    iadetails = mydb.dhi_internal.aggregate([
        {
            "$unwind": '$studentScores'
        },
        {"$unwind":"$faculties"},
        {"$unwind": '$departments'},
        {"$unwind": '$studentScores.evaluationParameterScore'},
        {
            "$match":
            {
                "faculties.facultyGivenId":fac_id,
                'academicYear':year,
                'courseCode':courseCode,
                'studentScores.section':section,
                'departments.deptId':deptId,
                'studentScores.termNumber':terms

            }},

        {
            "$group":
            {
                '_id': '$iaNumber',
                "maxMarks": {"$addToSet": "$studentScores.evaluationParameterScore.maxMarks"},
                "iaNumber": {"$addToSet": "$iaNumber"},
                "obtainedMarks": {"$addToSet": "$studentScores.totalScore"},
                "startTime": {"$addToSet": "$startTime"}
              
            }
        },
        {"$unwind": '$maxMarks'},
        {"$unwind": '$iaNumber'},
        {"$unwind": '$startTime'},
        {"$unwind": '$obtainedMarks'},
        {
           "$project":
                {
                    "_id": 0,
                    "maxMarks": "$maxMarks",
                    "obtainedMarks": "$obtainedMarks",
                    "iaNumber": "$iaNumber"
                    
                }
        }

    ])
    ia1_avg=0
    ia2_avg=0
    ia3_avg=0
    count1=1
    count2=1
    count3=1
    for x in iadetails:
        li1=[]
        li2=[]
        li3=[]
        
        iaNumber = x['iaNumber']
        maxMarks = x['maxMarks']
        if x['iaNumber'] == 1:
            li1.append(x['obtainedMarks'])
            count1 += 1
        for i in li1:
            ia1_avg+=i
        if x['iaNumber'] == 2:
            li2.append(x['obtainedMarks'])
            count2 += 1
        for i in li2:
            ia2_avg += i

        if x['iaNumber'] == 1:
            li3.append(x['obtainedMarks'])
            count3 += 1
        for i in li3:
            ia3_avg +=i 
    iaf_avg=ia1_avg/count1
    ias_avg=ia2_avg/count2
    iat_avg=ia3_avg/count3
    iaf_per=iaf_avg/maxMarks *100
    ias_per=ias_avg/maxMarks *100
    iat_per=iat_avg/maxMarks *100
    li=[]
    new={}
    new['maxMarks']=maxMarks
    new['iaf_avg']=round(iaf_avg,2)
    new['ias_avg']=round(ias_avg,2)
    new['iat_avg']=round(iat_avg,2)
    new['iaf_per']=round(iaf_per,2)
    new['ias_per']=round(ias_per,2)
    new['iat_per']=round(iat_per,2)
 
    li.append(new)
    
    return li
    
        
faculty_ia_details("CIV598","2017-18","15CV34","A","CV","3")          

        

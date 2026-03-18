# 🗳️ הגדרת מערכת ההצבעות – מדריך צעד אחר צעד

זמן משוער: **10 דקות**

---

## שלב 1 – פתח Google Cloud Console

גש ל: https://console.cloud.google.com

אם אין לך פרויקט, צור אחד (כפתור "New Project" למעלה).

---

## שלב 2 – הפעל את Google Sheets API

1. בתפריט הצד: **APIs & Services → Library**
2. חפש: `Google Sheets API`
3. לחץ **Enable**

---

## שלב 3 – צור Service Account

1. בתפריט הצד: **APIs & Services → Credentials**
2. לחץ **+ CREATE CREDENTIALS → Service account**
3. תן שם כלשהו (למשל: `profit-optimizer-bot`)
4. לחץ **Create and Continue** ואחר כך **Done**

---

## שלב 4 – צור מפתח JSON

1. תחת "Service Accounts" תראה את ה-account שיצרת – לחץ עליו
2. לשונית **Keys** ← **Add Key** ← **Create new key**
3. בחר **JSON** ← לחץ **Create**
4. הקובץ יורד אוטומטית למחשב שלך (נראה כמו: `profit-optimizer-bot-xxxx.json`)

---

## שלב 5 – שתף את ה-Spreadsheet עם ה-Service Account

1. פתח את קובץ ה-JSON שהורדת
2. מצא את השדה `"client_email"` – הוא נראה כמו:
   ```
   profit-optimizer-bot@your-project.iam.gserviceaccount.com
   ```
3. פתח את ה-Google Spreadsheet שלך (קובץ הקרנות)
4. לחץ **שיתוף** (כפתור כחול למעלה מימין)
5. הוסף את ה-email הזה עם הרשאת **Editor**
6. לחץ שלח / סיום

---

## שלב 6 – הוסף ל-Streamlit Secrets

1. גש ל-Streamlit Cloud: https://share.streamlit.io
2. בחר את האפליקציה שלך ← **Settings** ← **Secrets**
3. הדבק את הבלוק הבא (החלף עם התוכן האמיתי מקובץ ה-JSON):

```toml
[gcp_service_account]
type = "service_account"
project_id = "YOUR_PROJECT_ID"
private_key_id = "YOUR_PRIVATE_KEY_ID"
private_key = "-----BEGIN RSA PRIVATE KEY-----\nYOUR_KEY_HERE\n-----END RSA PRIVATE KEY-----\n"
client_email = "profit-optimizer-bot@YOUR_PROJECT.iam.gserviceaccount.com"
client_id = "YOUR_CLIENT_ID"
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/YOUR_EMAIL"
```

> 💡 **טיפ:** פתח את קובץ ה-JSON בעורך טקסט, העתק כל שדה לפי שמו.
> שים לב ש-`private_key` צריך לשמור על ה-`\n` בתוכו.

4. לחץ **Save**
5. האפליקציה תיטען מחדש אוטומטית – ההצבעות מופעלות! ✅

---

## מה קורה אחרי ההגדרה?

- בטאב "תוצאות" יופיעו כפתורי הצבעה מתחת לטבלה
- כל הצבעה נכתבת לגיליון `votes` ב-Spreadsheet שלך
- לחיצה על "סטטיסטיקת בחירות כל המשתמשים" תציג:
  - כמה הצבעות היו ב-7 / 30 יום האחרונים
  - אילו חלופות פופולריות יותר
  - אילו מסלולים נבחרו הכי הרבה

## פרטיות

- **לא** נאסף שום מידע מזהה (ללא IP, ללא שם, ללא email)
- כל הצבעה מקבלת hash אנונימי של 10 תווים בלבד
- הנתונים נשמרים אצלך ב-Google Sheets – לא אצל גורם שלישי

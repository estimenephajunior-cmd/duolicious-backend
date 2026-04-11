def otp_template(otp: str) -> str:
    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Sign in</title>
</head>

<body style="margin:0; padding:0; font-family:'Segoe UI', Arial, sans-serif; background-color:#f6f2ff;">
    
<table width="100%" cellpadding="0" cellspacing="0" border="0">
<tr>
<td align="center">

<table width="600" cellpadding="0" cellspacing="0" border="0" style="max-width:600px; width:100%; border-radius:16px; overflow:hidden;">

    <tr>
        <td align="center" style="
            background: linear-gradient(135deg, #6a00ff, #b266ff);
            padding: 25px 20px;
            color: white;
        ">
            <div style="
                font-size: 28px;
                font-weight: 800;
                letter-spacing: 1px;
                font-family: 'Segoe UI', Arial, sans-serif;
            ">
                Jw<span style="color:#ffd6ff;">Boo</span>
            </div>
        </td>
    </tr>

    <tr>
        <td align="center" style="
            background-color:#ffffff;
            padding:40px 30px;
        ">
            <p style="
                font-size:18px;
                color:#5a2d91;
                margin:0 0 20px 0;
            ">
                Your one-time password
            </p>

            <div style="
                display:inline-block;
                background: linear-gradient(135deg, #6a00ff, #b266ff);
                padding:20px 35px;
                border-radius:14px;
                font-size:32px;
                font-weight:800;
                letter-spacing:4px;
                color:white;
                box-shadow:0 6px 20px rgba(106,0,255,0.25);
            ">
                {otp}
            </div>

            <p style="
                margin:25px 0 0 0;
                font-size:14px;
                color:#777;
            ">
                This code will expire shortly. Do not share it with anyone.
            </p>

        </td>
    </tr>

    <tr>
        <td align="center" style="
            background: linear-gradient(135deg, #6a00ff, #8a33ff);
            padding:15px;
            font-size:12px;
            color:#e6ccff;
        ">
            If you didn&apos;t request this, you can safely ignore this email.
        </td>
    </tr>

</table>

</td>
</tr>
</table>

</body>
</html>
"""

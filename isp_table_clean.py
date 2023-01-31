query = """
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- JANET J
    Update isp
    Set Individual='James, Janet M'
    Where Individual like 'James, Janet'
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- CHRISTINA C
    Update isp
    Set Individual='Chituck, Christina L'
    Where Individual like 'Chituck, Christi%'
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- BRIANNA W.
    Update isp
    Set Individual='Wooters, Brianna E'
    Where Individual like 'Wooters, Bri%'
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- RALPH W.
    Update isp
    Set Individual='Wright, Ralph W'
    Where Individual like 'Wright, Ralph%'
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- ROBERT S.
    Update isp
    Set Individual='Seward, Robert'
    Where Individual like 'Seward, Rob%'
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- CHARLES L. 
    Update isp
    Set Individual='LeVan, Charles J'
    Where Individual like 'LeVan, Ch%'
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- JOSEPH G.
    Update isp
    Set Individual='GREEN, JOSEPH E E'
    Where Individual like 'GREEN, JOSE%'
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- JAMES G.
    Update isp
    Set Individual='Gallagher, James M'
    Where Individual like 'GALLAGHER%'
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- CHRISTIAN G.
    Update isp
    Set Individual='Garrison, Christian'
    Where Individual like 'GARRISON%'
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- DANIEL L.
    Update isp
    Set Individual='Lanier, Daniel L'
    Where Individual like 'LANIER%'
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- DULCE JR.
    Update isp
    Set Individual='Jardon-Rosales, Dulce Y'
    Where Individual like 'JARDON%'
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- DEVEN H.
    Update isp
    Set Individual='Headen, Deven T'
    Where Individual like 'HEAD%'
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- TRAVIS F.
    Update isp
    Set Individual='Faust, Travis A'
    Where Individual like 'FAUST%'
-- UPDATE COUNTY ISP TABLE TO REFLECT ATTENDANCE -- NYEA G.
    Update isp
    Set Individual='Goldsberry, Nyea Nicole'
    Where Individual like 'GOLDSBERRY%'
        """

##pandas cleaning


#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os

from osgeo import gdal, ogr

def EQUAL(a, b):
    return a.lower() == b.lower()

###############################################################################
# Redefinition of GDALTermProgress, so that autotest/pyscripts/test_ogr2ogr_py.py
# can check that the progress bar is displayed

nLastTick = -1

class TargetLayerInfo:
    def __init__(self):
        self.poDstLayer = None
        self.poCT = None
        #self.papszTransformOptions = None
        self.panMap = None
        self.iSrcZField = -1



bSkipFailures = False
nGroupTransactions = 20000
bPreserveFID = False
nFIDToFetch = ogr.NullFID

class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError

GeomOperation = Enum(["NONE", "SEGMENTIZE", "SIMPLIFY_PRESERVE_TOPOLOGY"])

def CSLFindString(v, mystr):
    i = 0
    for strIter in v:
        if EQUAL(strIter, mystr):
            return i
        i = i + 1
    return -1

def IsNumber( pszStr):
    try:
        (float)(pszStr)
        return True
    except:
        return False

def wkbFlatten(x):
    return x & (~ogr.wkb25DBit)

#/************************************************************************/
#/*                         SetupTargetLayer()                           */
#/************************************************************************/

def SetupTargetLayer( poSrcLayer, poDstDS, papszLCO = [], pszNewLayerName = None, \
                    poOutputSRS = None, bNullifyOutputSRS = False, poSourceSRS = None, papszSelFields = None, \
                    bAppend = True, eGType = -2, nCoordDim = -1, bOverwrite = False, papszFieldTypesToString = None, \
                    bWrapDateline = False, bExplodeCollections = False, pszZField = None, pszWHERE = None) :

    if pszNewLayerName is None:
        pszNewLayerName = poSrcLayer.GetLayerDefn().GetName()

#/* -------------------------------------------------------------------- */
#/*      Setup coordinate transformation if we need it.                  */
#/* -------------------------------------------------------------------- */
    poCT = None

#/* -------------------------------------------------------------------- */
#/*      Get other info.                                                 */
#/* -------------------------------------------------------------------- */
    poSrcFDefn = poSrcLayer.GetLayerDefn()

    if poOutputSRS is None and not bNullifyOutputSRS:
        poOutputSRS = poSrcLayer.GetSpatialRef()

#/* -------------------------------------------------------------------- */
#/*      Find the layer.                                                 */
#/* -------------------------------------------------------------------- */

    #/* GetLayerByName() can instanciate layers that would have been */
    #*/ 'hidden' otherwise, for example, non-spatial tables in a */
    #*/ Postgis-enabled database, so this apparently useless command is */
    #/* not useless... (#4012) */
    gdal.PushErrorHandler('CPLQuietErrorHandler')
    poDstLayer = poDstDS.GetLayerByName(pszNewLayerName)
    gdal.PopErrorHandler()
    gdal.ErrorReset()

    iLayer = -1
    if poDstLayer is not None:
        nLayerCount = poDstDS.GetLayerCount()
        for iLayer in range(nLayerCount):
            poLayer = poDstDS.GetLayer(iLayer)
            # The .cpp version compares on pointers directly, but we cannot
            # do this with swig object, so just compare the names.
            if poLayer is not None \
                and poLayer.GetName() == poDstLayer.GetName():
                break

        if (iLayer == nLayerCount):
            # /* shouldn't happen with an ideal driver */
            poDstLayer = None

#/* -------------------------------------------------------------------- */
#/*      If the user requested overwrite, and we have the layer in       */
#/*      question we need to delete it now so it will get recreated      */
#/*      (overwritten).                                                  */
#/* -------------------------------------------------------------------- */
    if poDstLayer is not None and bOverwrite:
        if poDstDS.DeleteLayer( iLayer ) != 0:
            print("DeleteLayer() failed when overwrite requested." )
            return None

        poDstLayer = None

#/* -------------------------------------------------------------------- */
#/*      If the layer does not exist, then create it.                    */
#/* -------------------------------------------------------------------- */
    if poDstLayer is None:
        if eGType == -2:
            eGType = poSrcFDefn.GetGeomType()

            if bExplodeCollections:
                n25DBit = eGType & ogr.wkb25DBit
                if wkbFlatten(eGType) == ogr.wkbMultiPoint:
                    eGType = ogr.wkbPoint | n25DBit
                elif wkbFlatten(eGType) == ogr.wkbMultiLineString:
                    eGType = ogr.wkbLineString | n25DBit
                elif wkbFlatten(eGType) == ogr.wkbMultiPolygon:
                    eGType = ogr.wkbPolygon | n25DBit
                elif wkbFlatten(eGType) == ogr.wkbGeometryCollection:
                    eGType = ogr.wkbUnknown | n25DBit

            if pszZField is not None:
                eGType = eGType | ogr.wkb25DBit

        if nCoordDim == 2:
            eGType = eGType & ~ogr.wkb25DBit
        elif nCoordDim == 3:
            eGType = eGType | ogr.wkb25DBit

        if poDstDS.TestCapability( ogr.ODsCCreateLayer ) == False:
            print("Layer " + pszNewLayerName + "not found, and CreateLayer not supported by driver.")
            return None

        gdal.ErrorReset()

        poDstLayer = poDstDS.CreateLayer( pszNewLayerName, poOutputSRS, \
                                            eGType, papszLCO )

        if poDstLayer is None:
            return None

        bAppend = False

#/* -------------------------------------------------------------------- */
#/*      Otherwise we will append to it, if append was requested.        */
#/* -------------------------------------------------------------------- */
    elif not bAppend:
        print("FAILED: Layer " + pszNewLayerName + "already exists, and -append not specified.\n" + \
                            "        Consider using -append, or -overwrite.")
        return None
    else:
        if len(papszLCO) > 0:
            print("WARNING: Layer creation options ignored since an existing layer is\n" + \
                    "         being appended to." )

#/* -------------------------------------------------------------------- */
#/*      Add fields.  Default to copy all field.                         */
#/*      If only a subset of all fields requested, then output only      */
#/*      the selected fields, and in the order that they were            */
#/*      selected.                                                       */
#/* -------------------------------------------------------------------- */

    # Initialize the index-to-index map to -1's
    nSrcFieldCount = poSrcFDefn.GetFieldCount()
    panMap = [ -1 for i in range(nSrcFieldCount) ]

    poDstFDefn = poDstLayer.GetLayerDefn()

    if papszSelFields is not None and not bAppend:

        nDstFieldCount = 0
        if poDstFDefn is not None:
            nDstFieldCount = poDstFDefn.GetFieldCount()

        for iField in range(len(papszSelFields)):

            iSrcField = poSrcFDefn.GetFieldIndex(papszSelFields[iField])
            if iSrcField >= 0:
                poSrcFieldDefn = poSrcFDefn.GetFieldDefn(iSrcField)
                oFieldDefn = ogr.FieldDefn( poSrcFieldDefn.GetNameRef(),
                                            poSrcFieldDefn.GetType() )
                oFieldDefn.SetWidth( poSrcFieldDefn.GetWidth() )
                oFieldDefn.SetPrecision( poSrcFieldDefn.GetPrecision() )

                if papszFieldTypesToString is not None and \
                    (CSLFindString(papszFieldTypesToString, "All") != -1 or \
                    CSLFindString(papszFieldTypesToString, \
                                ogr.GetFieldTypeName(poSrcFieldDefn.GetType())) != -1):

                    oFieldDefn.SetType(ogr.OFTString)

                # The field may have been already created at layer creation
                iDstField = -1
                if poDstFDefn is not None:
                    iDstField = poDstFDefn.GetFieldIndex(oFieldDefn.GetNameRef())
                if iDstField >= 0:
                    panMap[iSrcField] = iDstField
                elif poDstLayer.CreateField( oFieldDefn ) == 0:
                    # now that we've created a field, GetLayerDefn() won't return NULL
                    if poDstFDefn is None:
                        poDstFDefn = poDstLayer.GetLayerDefn()

                    #/* Sanity check : if it fails, the driver is buggy */
                    if poDstFDefn is not None and \
                        poDstFDefn.GetFieldCount() != nDstFieldCount + 1:
                        print("The output driver has claimed to have added the %s field, but it did not!" %  oFieldDefn.GetNameRef() )
                    else:
                        panMap[iSrcField] = nDstFieldCount
                        nDstFieldCount = nDstFieldCount + 1

            else:
                print("Field '" + papszSelFields[iField] + "' not found in source layer.")
                if not bSkipFailures:
                    return None

        #/* -------------------------------------------------------------------- */
        #/* Use SetIgnoredFields() on source layer if available                  */
        #/* -------------------------------------------------------------------- */

        # Here we differ from the ogr2ogr.cpp implementation since the OGRFeatureQuery
        # isn't mapped to swig. So in that case just don't use SetIgnoredFields()
        # to avoid issue raised in #4015
        if poSrcLayer.TestCapability(ogr.OLCIgnoreFields) and pszWHERE is None:
            papszIgnoredFields = []
            for iSrcField in range(nSrcFieldCount):
                pszFieldName = poSrcFDefn.GetFieldDefn(iSrcField).GetNameRef()
                bFieldRequested = False
                for iField in range(len(papszSelFields)):
                    if EQUAL(pszFieldName, papszSelFields[iField]):
                        bFieldRequested = True
                        break

                if pszZField is not None and EQUAL(pszFieldName, pszZField):
                    bFieldRequested = True

                #/* If source field not requested, add it to ignored files list */
                if not bFieldRequested:
                    papszIgnoredFields.append(pszFieldName)

            poSrcLayer.SetIgnoredFields(papszIgnoredFields)

    elif not bAppend:

        nDstFieldCount = 0
        if poDstFDefn is not None:
            nDstFieldCount = poDstFDefn.GetFieldCount()

        for iField in range(nSrcFieldCount):

            poSrcFieldDefn = poSrcFDefn.GetFieldDefn(iField)
            oFieldDefn = ogr.FieldDefn( poSrcFieldDefn.GetNameRef(),
                                        poSrcFieldDefn.GetType() )
            oFieldDefn.SetWidth( poSrcFieldDefn.GetWidth() )
            oFieldDefn.SetPrecision( poSrcFieldDefn.GetPrecision() )

            if papszFieldTypesToString is not None and \
                (CSLFindString(papszFieldTypesToString, "All") != -1 or \
                CSLFindString(papszFieldTypesToString, \
                            ogr.GetFieldTypeName(poSrcFieldDefn.GetType())) != -1):

                oFieldDefn.SetType(ogr.OFTString)

            # The field may have been already created at layer creation
            iDstField = -1
            if poDstFDefn is not None:
                 iDstField = poDstFDefn.GetFieldIndex(oFieldDefn.GetNameRef())
            if iDstField >= 0:
                panMap[iField] = iDstField
            elif poDstLayer.CreateField( oFieldDefn ) == 0:
                # now that we've created a field, GetLayerDefn() won't return NULL
                if poDstFDefn is None:
                    poDstFDefn = poDstLayer.GetLayerDefn()

                #/* Sanity check : if it fails, the driver is buggy */
                if poDstFDefn is not None and \
                    poDstFDefn.GetFieldCount() != nDstFieldCount + 1:
                    print("The output driver has claimed to have added the %s field, but it did not!" %  oFieldDefn.GetNameRef() )
                else:
                    panMap[iField] = nDstFieldCount
                    nDstFieldCount = nDstFieldCount + 1

    else:
        #/* For an existing layer, build the map by fetching the index in the destination */
        #/* layer for each source field */
        if poDstFDefn is None:
            print( "poDstFDefn == NULL.\n" )
            return None

        for iField in range(nSrcFieldCount):
            poSrcFieldDefn = poSrcFDefn.GetFieldDefn(iField)
            iDstField = poDstFDefn.GetFieldIndex(poSrcFieldDefn.GetNameRef())
            if iDstField >= 0:
                panMap[iField] = iDstField

    iSrcZField = -1
    if pszZField is not None:
        iSrcZField = poSrcFDefn.GetFieldIndex(pszZField)

    psInfo = TargetLayerInfo()
    psInfo.poDstLayer = poDstLayer
    psInfo.poCT = poCT
    #psInfo.papszTransformOptions = papszTransformOptions
    psInfo.panMap = panMap
    psInfo.iSrcZField = iSrcZField

    return psInfo


def newSetupTargetLayer( poSrcLayer, poDstDS, bAppend = True, bOverwrite = False) :

	pszNewLayerName = poSrcLayer.GetLayerDefn().GetName()

#/* -------------------------------------------------------------------- */
#/*      Setup coordinate transformation if we need it.                  */
#/* -------------------------------------------------------------------- */
	poCT = None
	papszLCO = []
#/* -------------------------------------------------------------------- */
#/*      Get other info.                                                 */
#/* -------------------------------------------------------------------- */
	poSrcFDefn = poSrcLayer.GetLayerDefn()

	poOutputSRS = poSrcLayer.GetSpatialRef()

#/* -------------------------------------------------------------------- */
#/*      Find the layer.                                                 */
#/* -------------------------------------------------------------------- */

    #/* GetLayerByName() can instanciate layers that would have been */
    #*/ 'hidden' otherwise, for example, non-spatial tables in a */
    #*/ Postgis-enabled database, so this apparently useless command is */
    #/* not useless... (#4012) */
	gdal.PushErrorHandler('CPLQuietErrorHandler')
	poDstLayer = poDstDS.GetLayerByName(pszNewLayerName)
	gdal.PopErrorHandler()
	gdal.ErrorReset()

	iLayer = -1
	if poDstLayer is not None:
		nLayerCount = poDstDS.GetLayerCount()
		for iLayer in range(nLayerCount):
			poLayer = poDstDS.GetLayer(iLayer)
			# The .cpp version compares on pointers directly, but we cannot
			# do this with swig object, so just compare the names.
			if poLayer is not None \
				and poLayer.GetName() == poDstLayer.GetName():
				break

		if (iLayer == nLayerCount):
			# /* shouldn't happen with an ideal driver */
			poDstLayer = None

#/* -------------------------------------------------------------------- */
#/*      If the user requested overwrite, and we have the layer in       */
#/*      question we need to delete it now so it will get recreated      */
#/*      (overwritten).                                                  */
#/* -------------------------------------------------------------------- */
	if poDstLayer is not None and bOverwrite:
		if poDstDS.DeleteLayer( iLayer ) != 0:
			print("DeleteLayer() failed when overwrite requested." )
			return None

		poDstLayer = None

#/* -------------------------------------------------------------------- */
#/*      If the layer does not exist, then create it.                    */
#/* -------------------------------------------------------------------- */
	if poDstLayer is None:
		eGType = poSrcFDefn.GetGeomType()

		if poDstDS.TestCapability( ogr.ODsCCreateLayer ) == False:
			print("Layer " + pszNewLayerName + "not found, and CreateLayer not supported by driver.")
			return None

		gdal.ErrorReset()

		poDstLayer = poDstDS.CreateLayer( pszNewLayerName, poOutputSRS, \
											eGType, papszLCO )

		if poDstLayer is None:
			return None

		bAppend = False

#/* -------------------------------------------------------------------- */
#/*      Otherwise we will append to it, if append was requested.        */
#/* -------------------------------------------------------------------- */
	elif not bAppend:
		print("FAILED: Layer " + pszNewLayerName + "already exists, and -append not specified.\n" + \
							"        Consider using -append, or -overwrite.")
		return None
	else:
		if len(papszLCO) > 0:
			print("WARNING: Layer creation options ignored since an existing layer is\n" + \
					"         being appended to." )

#/* -------------------------------------------------------------------- */
#/*      Add fields.  Default to copy all field.                         */
#/*      If only a subset of all fields requested, then output only      */
#/*      the selected fields, and in the order that they were            */
#/*      selected.                                                       */
#/* -------------------------------------------------------------------- */

    # Initialize the index-to-index map to -1's
	nSrcFieldCount = poSrcFDefn.GetFieldCount()
	panMap = [ -1 for i in range(nSrcFieldCount) ]

	poDstFDefn = poDstLayer.GetLayerDefn()

	if not bAppend:

		nDstFieldCount = 0
		if poDstFDefn is not None:
			nDstFieldCount = poDstFDefn.GetFieldCount()

		for iField in range(nSrcFieldCount):

			poSrcFieldDefn = poSrcFDefn.GetFieldDefn(iField)
			oFieldDefn = ogr.FieldDefn( poSrcFieldDefn.GetNameRef(),
										poSrcFieldDefn.GetType() )
			oFieldDefn.SetWidth( poSrcFieldDefn.GetWidth() )
			oFieldDefn.SetPrecision( poSrcFieldDefn.GetPrecision() )

			# The field may have been already created at layer creation
			iDstField = -1
			if poDstFDefn is not None:
				 iDstField = poDstFDefn.GetFieldIndex(oFieldDefn.GetNameRef())
			if iDstField >= 0:
				panMap[iField] = iDstField
			elif poDstLayer.CreateField( oFieldDefn ) == 0:
				# now that we've created a field, GetLayerDefn() won't return NULL
				if poDstFDefn is None:
					poDstFDefn = poDstLayer.GetLayerDefn()

				#/* Sanity check : if it fails, the driver is buggy */
				if poDstFDefn is not None and \
					poDstFDefn.GetFieldCount() != nDstFieldCount + 1:
					print("The output driver has claimed to have added the %s field, but it did not!" %  oFieldDefn.GetNameRef() )
				else:
					panMap[iField] = nDstFieldCount
					nDstFieldCount = nDstFieldCount + 1

	else:
		#/* For an existing layer, build the map by fetching the index in the destination */
		#/* layer for each source field */
		if poDstFDefn is None:
			print( "poDstFDefn == NULL.\n" )
			return None

		for iField in range(nSrcFieldCount):
			poSrcFieldDefn = poSrcFDefn.GetFieldDefn(iField)
			iDstField = poDstFDefn.GetFieldIndex(poSrcFieldDefn.GetNameRef())
			if iDstField >= 0:
				panMap[iField] = iDstField

	iSrcZField = -1

	psInfo = TargetLayerInfo()
	psInfo.poDstLayer = poDstLayer
	psInfo.poCT = poCT
	#psInfo.papszTransformOptions = papszTransformOptions
	psInfo.panMap = panMap
	psInfo.iSrcZField = iSrcZField

	return psInfo



#/************************************************************************/
#/*                           TranslateLayer()                           */
#/************************************************************************/
def newTranslateLayer( psInfo, poDstDS, poSrcLayer):

    poDstLayer = psInfo.poDstLayer
    poCT = psInfo.poCT
    panMap = psInfo.panMap
    iSrcZField = psInfo.iSrcZField

#/* -------------------------------------------------------------------- */
#/*      Transfer features.                                              */
#/* -------------------------------------------------------------------- */
    nFeaturesInTransaction = 0

    if nGroupTransactions > 0:
        poDstLayer.StartTransaction()

    while True:
		poDstFeature = None

		poFeature = poSrcLayer.GetNextFeature()

		if poFeature is None:
			break

		nFeaturesInTransaction = nFeaturesInTransaction + 1
		if nFeaturesInTransaction == nGroupTransactions:
			poDstLayer.CommitTransaction()
			poDstLayer.StartTransaction()
			nFeaturesInTransaction = 0

		poDstFeature = ogr.Feature( poDstLayer.GetLayerDefn() )

		if poDstFeature.SetFromWithMap( poFeature, 1, panMap ) != 0:

			if nGroupTransactions > 0:
				poDstLayer.CommitTransaction()
			print("Unable to translate feature %d from layer %s" % (poFeature.GetFID() , poSrcLayer.GetName() ))
			return False

		gdal.ErrorReset()
		if poDstLayer.CreateFeature( poDstFeature ) != 0 and not bSkipFailures:
			if nGroupTransactions > 0:
				poDstLayer.RollbackTransaction()

			return False

    if nGroupTransactions > 0:
        poDstLayer.CommitTransaction()

    return True
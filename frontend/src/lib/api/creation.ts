import apiClient from './client'
import { insightsApi } from './insights'
import {
  CreationArtifact,
  CreatorsResponse,
  GenerateCreationRequest,
  GenerateCreationResponse,
} from '@/lib/types/creation'

export const creationApi = {
  listCreators: async () => {
    const response = await apiClient.get<CreatorsResponse>('/creation/creators')
    return response.data
  },

  listArtifacts: async (params: { creator_key?: string; notebook_id?: string }) => {
    const response = await apiClient.get<CreationArtifact[]>('/creation/artifacts', {
      params,
    })
    return response.data
  },

  getArtifact: async (id: string) => {
    const response = await apiClient.get<CreationArtifact>(`/creation/artifacts/${id}`)
    return response.data
  },

  generate: async (payload: GenerateCreationRequest) => {
    const response = await apiClient.post<GenerateCreationResponse>(
      '/creation/artifacts/generate',
      payload
    )
    return response.data
  },

  deleteArtifact: async (id: string) => {
    await apiClient.delete(`/creation/artifacts/${id}`)
  },

  downloadFile: async (artifactId: string, fileIndex: number): Promise<Blob> => {
    const response = await apiClient.get<Blob>(
      `/creation/artifacts/${artifactId}/files/${fileIndex}`,
      { responseType: 'blob' }
    )
    return response.data
  },

  getReviewState: async (artifactId: string) => {
    const response = await apiClient.get<Record<string, Record<string, unknown>>>(
      `/creation/artifacts/${artifactId}/review`
    )
    return response.data
  },

  saveReviewState: async (
    artifactId: string,
    states: Record<string, Record<string, unknown>>
  ) => {
    await apiClient.put(`/creation/artifacts/${artifactId}/review`, { states })
  },

  // Reuse the generic surreal-commands job poller.
  waitForCommand: insightsApi.waitForCommand,
}

export function triggerBrowserDownload(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(url)
}

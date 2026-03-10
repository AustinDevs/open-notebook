'use client'

import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { Card, CardContent, CardHeader, CardTitle, CardDescription, CardFooter } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { LoadingSpinner } from '@/components/common/LoadingSpinner'
import { Alert, AlertTitle, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import {
  useS3Status,
  useS3Config,
  useSaveS3Config,
  useDeleteS3Config,
  useTestS3Connection,
} from '@/lib/hooks/use-s3-config'
import { useEffect, useState } from 'react'
import { CheckCircle, XCircle, Cloud, HardDrive, AlertTriangle, Trash2, FlaskConical } from 'lucide-react'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/components/ui/alert-dialog'

const s3ConfigSchema = z.object({
  access_key_id: z.string().min(1, 'Access Key ID is required'),
  secret_access_key: z.string().min(1, 'Secret Access Key is required'),
  bucket_name: z.string().min(1, 'Bucket name is required'),
  region: z.string(),
  endpoint_url: z.string(),
  public_url: z.string(),
  use_path_style: z.boolean(),
})

type S3ConfigFormData = z.infer<typeof s3ConfigSchema>

function StatusBadge({ source }: { source: string }) {
  if (source === 'database') {
    return (
      <Badge variant="default" className="flex items-center gap-1">
        <Cloud className="h-3 w-3" />
        Configured via Settings
      </Badge>
    )
  }
  if (source === 'environment') {
    return (
      <Badge variant="secondary" className="flex items-center gap-1">
        <HardDrive className="h-3 w-3" />
        Configured via Environment
      </Badge>
    )
  }
  return (
    <Badge variant="outline" className="flex items-center gap-1">
      <XCircle className="h-3 w-3" />
      Not Configured
    </Badge>
  )
}

export function S3SettingsForm() {
  const { data: status, isLoading: statusLoading, error: statusError } = useS3Status()
  const { data: config, isLoading: configLoading } = useS3Config()
  const saveConfig = useSaveS3Config()
  const deleteConfig = useDeleteS3Config()
  const testConnection = useTestS3Connection()
  const [hasResetForm, setHasResetForm] = useState(false)

  const {
    register,
    handleSubmit,
    reset,
    watch,
    setValue,
    formState: { errors, isDirty },
  } = useForm<S3ConfigFormData>({
    resolver: zodResolver(s3ConfigSchema),
    defaultValues: {
      access_key_id: '',
      secret_access_key: '',
      bucket_name: '',
      region: 'us-east-1',
      endpoint_url: '',
      public_url: '',
      use_path_style: false,
    },
  })

  const usePathStyle = watch('use_path_style')

  useEffect(() => {
    if (config && !hasResetForm) {
      reset({
        access_key_id: '', // Never pre-fill secrets
        secret_access_key: '',
        bucket_name: config.bucket_name || '',
        region: config.region || 'us-east-1',
        endpoint_url: config.endpoint_url || '',
        public_url: config.public_url || '',
        use_path_style: config.use_path_style || false,
      })
      setHasResetForm(true)
    }
  }, [config, hasResetForm, reset])

  const onSubmit = async (data: S3ConfigFormData) => {
    await saveConfig.mutateAsync({
      access_key_id: data.access_key_id,
      secret_access_key: data.secret_access_key,
      bucket_name: data.bucket_name,
      region: data.region || 'us-east-1',
      endpoint_url: data.endpoint_url || undefined,
      public_url: data.public_url || undefined,
      use_path_style: data.use_path_style,
    })
    // Clear the form secrets after save
    setValue('access_key_id', '')
    setValue('secret_access_key', '')
  }

  const handleDelete = async () => {
    await deleteConfig.mutateAsync()
  }

  const handleTestConnection = async () => {
    await testConnection.mutateAsync()
  }

  if (statusLoading || configLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <LoadingSpinner size="lg" />
      </div>
    )
  }

  if (statusError) {
    return (
      <Alert variant="destructive">
        <AlertTitle>Failed to load S3 status</AlertTitle>
        <AlertDescription>
          {statusError instanceof Error ? statusError.message : 'Unknown error'}
        </AlertDescription>
      </Alert>
    )
  }

  const isConfiguredViaEnv = status?.source === 'environment'
  const isConfiguredViaDb = status?.source === 'database'

  return (
    <div className="space-y-6">
      {/* Status Card */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-3">
            Storage Status
            <StatusBadge source={status?.source || 'none'} />
          </CardTitle>
          <CardDescription>
            S3 storage allows you to store uploaded files and podcast audio in cloud storage
            instead of local disk. This is useful for multi-instance deployments.
          </CardDescription>
        </CardHeader>
        {status?.configured && (
          <CardContent>
            <div className="text-sm text-muted-foreground space-y-1">
              <p><strong>Bucket:</strong> {status.bucket_name}</p>
              {status.region && <p><strong>Region:</strong> {status.region}</p>}
              {status.endpoint_url && <p><strong>Endpoint:</strong> {status.endpoint_url}</p>}
            </div>
          </CardContent>
        )}
        {status?.configured && (
          <CardFooter className="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={handleTestConnection}
              disabled={testConnection.isPending}
            >
              <FlaskConical className="h-4 w-4 mr-2" />
              {testConnection.isPending ? 'Testing...' : 'Test Connection'}
            </Button>
          </CardFooter>
        )}
      </Card>

      {/* Environment Variable Notice */}
      {isConfiguredViaEnv && (
        <Alert>
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Configured via Environment Variables</AlertTitle>
          <AlertDescription>
            S3 is currently configured using environment variables. You can override these
            settings by saving a configuration below, which will take priority.
          </AlertDescription>
        </Alert>
      )}

      {/* Configuration Form */}
      <form onSubmit={handleSubmit(onSubmit)}>
        <Card>
          <CardHeader>
            <CardTitle>S3 Configuration</CardTitle>
            <CardDescription>
              Configure S3-compatible storage credentials. Supports AWS S3, DigitalOcean Spaces,
              MinIO, Backblaze B2, and other S3-compatible services.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-6">
            {/* Credentials */}
            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label htmlFor="access_key_id">Access Key ID *</Label>
                <Input
                  id="access_key_id"
                  type="password"
                  placeholder={config?.has_credentials ? '••••••••' : 'Enter access key'}
                  {...register('access_key_id')}
                />
                {errors.access_key_id && (
                  <p className="text-sm text-destructive">{errors.access_key_id.message}</p>
                )}
              </div>
              <div className="space-y-2">
                <Label htmlFor="secret_access_key">Secret Access Key *</Label>
                <Input
                  id="secret_access_key"
                  type="password"
                  placeholder={config?.has_credentials ? '••••••••' : 'Enter secret key'}
                  {...register('secret_access_key')}
                />
                {errors.secret_access_key && (
                  <p className="text-sm text-destructive">{errors.secret_access_key.message}</p>
                )}
              </div>
            </div>

            {/* Bucket & Region */}
            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label htmlFor="bucket_name">Bucket Name *</Label>
                <Input
                  id="bucket_name"
                  placeholder="my-bucket"
                  {...register('bucket_name')}
                />
                {errors.bucket_name && (
                  <p className="text-sm text-destructive">{errors.bucket_name.message}</p>
                )}
              </div>
              <div className="space-y-2">
                <Label htmlFor="region">Region</Label>
                <Input
                  id="region"
                  placeholder="us-east-1"
                  {...register('region')}
                />
              </div>
            </div>

            {/* Endpoint URL (for S3-compatible services) */}
            <div className="space-y-2">
              <Label htmlFor="endpoint_url">Custom Endpoint URL</Label>
              <Input
                id="endpoint_url"
                placeholder="https://nyc3.digitaloceanspaces.com"
                {...register('endpoint_url')}
              />
              <p className="text-sm text-muted-foreground">
                Leave empty for AWS S3. Set for S3-compatible services like DigitalOcean Spaces or MinIO.
              </p>
            </div>

            {/* Public URL */}
            <div className="space-y-2">
              <Label htmlFor="public_url">Public URL Prefix</Label>
              <Input
                id="public_url"
                placeholder="https://my-bucket.nyc3.cdn.digitaloceanspaces.com"
                {...register('public_url')}
              />
              <p className="text-sm text-muted-foreground">
                Optional: Public URL prefix for serving files (e.g., CDN URL).
              </p>
            </div>

            {/* Path Style */}
            <div className="flex items-center gap-4 rounded-lg border p-4">
              <Checkbox
                id="use_path_style"
                checked={usePathStyle}
                onCheckedChange={(checked: boolean) => setValue('use_path_style', checked)}
              />
              <div className="space-y-0.5">
                <Label htmlFor="use_path_style">Use Path-Style URLs</Label>
                <p className="text-sm text-muted-foreground">
                  Required for some S3-compatible services like MinIO. Uses bucket/key instead of bucket.endpoint/key format.
                </p>
              </div>
            </div>
          </CardContent>
          <CardFooter className="flex justify-between">
            <div>
              {isConfiguredViaDb && (
                <AlertDialog>
                  <AlertDialogTrigger asChild>
                    <Button variant="destructive" type="button">
                      <Trash2 className="h-4 w-4 mr-2" />
                      Remove Configuration
                    </Button>
                  </AlertDialogTrigger>
                  <AlertDialogContent>
                    <AlertDialogHeader>
                      <AlertDialogTitle>Remove S3 Configuration?</AlertDialogTitle>
                      <AlertDialogDescription>
                        This will remove the S3 configuration from the database. If you have
                        environment variables configured, they will be used as a fallback.
                        Existing files in S3 will not be deleted.
                      </AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                      <AlertDialogCancel>Cancel</AlertDialogCancel>
                      <AlertDialogAction onClick={handleDelete}>
                        Remove Configuration
                      </AlertDialogAction>
                    </AlertDialogFooter>
                  </AlertDialogContent>
                </AlertDialog>
              )}
            </div>
            <Button type="submit" disabled={saveConfig.isPending}>
              {saveConfig.isPending ? 'Saving...' : 'Save Configuration'}
            </Button>
          </CardFooter>
        </Card>
      </form>
    </div>
  )
}
